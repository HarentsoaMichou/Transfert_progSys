package ServeurPrincipal;

import java.io.*;
import java.net.*;
import java.util.*;

public class ServeurPrincipal 
{
    private static String serverAddress;
    private static String serverPrincipalDirectory;
    private static int serverPort;
    private static List<String> secondaryServerAddresses = new ArrayList<>();
    private static List<Integer> secondaryServerPorts = new ArrayList<>();
    private static List<String> secondaryServerDirectories = new ArrayList<>();

    static {
        try (InputStream input = new FileInputStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(input);

            // Charger la configuration du serveur principal
            serverAddress = prop.getProperty("server.principal.address", "localhost");
            serverPort = Integer.parseInt(prop.getProperty("server.principal.port.base", "12345"));
            serverPrincipalDirectory = prop.getProperty("server.principal.directory", "ServeurPrincipal");

            // Charger la configuration des serveurs secondaires
            int numberOfSecondaryServers = Integer.parseInt(prop.getProperty("server.secondary.count", "2"));
            for (int i = 1; i <= numberOfSecondaryServers; i++) 
            {
                String secondaryAddress = prop.getProperty("server.secondary" + i + ".address", "localhost");
                int secondaryPort = Integer.parseInt(prop.getProperty("server.secondary" + i + ".port.base", "12346"));
                String secondaryDirectory = prop.getProperty("server.secondary" + i + ".directory", "ServeurSecondaire");

                secondaryServerAddresses.add(secondaryAddress);
                secondaryServerPorts.add(secondaryPort+i);
                secondaryServerDirectories.add(secondaryDirectory);

            }
            System.out.println("Nombre de serveurs secondaires chargés : " + secondaryServerAddresses.size());
        } catch (IOException ex) {
            System.err.println("Erreur lors du chargement de la configuration : " + ex.getMessage());
            System.exit(1);
        }
    }


    public static void main(String[] args) 
    {
        try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
            System.out.println("Serveur principal prêt à recevoir des commandes...");

            while (true) {
                // Accepter un client
                Socket clientSocket = serverSocket.accept();
                System.out.println("Connexion acceptée depuis : " + clientSocket.getInetAddress());

                // Traiter chaque client dans un nouveau thread
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Erreur au niveau du serveur principal : " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Classe interne pour gérer les connexions client
    private static class ClientHandler implements Runnable 
    {
        private final Socket clientSocket;

        public ClientHandler(Socket clientSocket) 
        {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() 
        {
            try (DataInputStream clientInputStream = new DataInputStream(clientSocket.getInputStream());
                 DataOutputStream clientOutputStream = new DataOutputStream(clientSocket.getOutputStream())) {

                // Lire la commande du client (PUT, GET, LS, RM)
                String command = clientInputStream.readUTF();
                System.out.println("Commande reçue : " + command);

                switch (command.toUpperCase()) {
                    case "PUT":
                        handlePut(clientInputStream, clientOutputStream);
                        break;
                    case "GET":
                        handleGet(clientInputStream, clientOutputStream);
                        break;
                    case "LS":
                        handleLs(clientOutputStream);
                        break;
                    case "RM":
                        handleRm(clientInputStream, clientOutputStream);
                        break;
                    default:
                        System.out.println("Commande inconnue : " + command);
                        clientOutputStream.writeUTF("Commande invalide.");
                        break;
                }

            } catch (IOException e) {
                // System.err.println("Erreur lors du traitement du client : " + e.getMessage());
                // e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Erreur lors de la fermeture du socket client : " + e.getMessage());
                }
            }
        }

        private static void handlePut(DataInputStream clientInputStream, DataOutputStream clientOutputStream) throws IOException 
        {
            // Recevoir le nom et la taille du fichier
            String fileName = clientInputStream.readUTF();
            long fileSize = clientInputStream.readLong();
        
            // Sauvegarder le fichier reçu
            File file = new File(serverPrincipalDirectory, "received_" + fileName);
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) 
            {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = clientInputStream.read(buffer)) != -1) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                }
            }
        
            System.out.println("Fichier reçu et sauvegardé : " + fileName);
        
            // Vérification de la disponibilité des serveurs secondaires
            List<Socket> secondaryServers = checkAvailableServers();
        
            // Diviser et envoyer aux serveurs secondaires disponibles
            divideAndSendFile(file, secondaryServers);
        
            // Fermer les connexions avec les serveurs secondaires
            for (Socket secondarySocket : secondaryServers) {
                if (!secondarySocket.isClosed()) {
                    secondarySocket.close();
                }
            }
        
            clientOutputStream.writeUTF("Fichier reçu et distribué avec succès.");
        }

        private static List<Socket> checkAvailableServers() {
            List<Socket> availableServers = new ArrayList<>();
            
            for (int i = 0; i < secondaryServerAddresses.size(); i++) {
                String serverAddress = secondaryServerAddresses.get(i);
                int serverPort = secondaryServerPorts.get(i);

                try {
                    Socket socket = new Socket(serverAddress, serverPort);
                    availableServers.add(socket);
                    System.out.println("Serveur secondaire " + (i + 1) + " disponible.");
                } catch (IOException e) {
                    System.err.println("Erreur de connexion au serveur secondaire " + (i + 1) + ": " + e.getMessage());
                }
            }
            return availableServers;
        }

        private static void divideAndSendFile(File file, List<Socket> availableServers) throws IOException {
            // Diviser et envoyer le fichier aux serveurs secondaires disponibles
            long fileSize = file.length();
            int nbServers = availableServers.size();
            if (nbServers == 0) {
                System.err.println("Aucun serveur secondaire disponible pour traiter le fichier.");
                return;
            }
            long partSize = fileSize / nbServers;
            long remainingBytes = fileSize % nbServers;

            FileInputStream fileInputStream = new FileInputStream(file);
            
            for (int i = 0; i < nbServers; i++) 
            {
                File partFile = new File(secondaryServerDirectories.get(i), "part_" + (i + 1) + "_" + file.getName());
                FileOutputStream partOutputStream = new FileOutputStream(partFile);
                byte[] buffer = new byte[1024];
                long bytesToWrite = partSize + (i == nbServers - 1 ? remainingBytes : 0);
                long bytesWritten = 0;
                
                while (bytesWritten < bytesToWrite) {
                    int bytesRead = fileInputStream.read(buffer, 0, (int) Math.min(buffer.length, bytesToWrite - bytesWritten));
                    if (bytesRead == -1) break;
                    partOutputStream.write(buffer, 0, bytesRead);
                    bytesWritten += bytesRead;
                }

                partOutputStream.close();
                System.out.println("Partie " + (i + 1) + " prête, taille : " + partFile.length() + " bytes");

                // Envoyer cette partie au serveur secondaire disponible
                sendFileToSecondaryServer(partFile, availableServers.get(i), i + 1);
            }
            fileInputStream.close();
        }

        private static void sendFileToSecondaryServer(File file, Socket secondarySocket, int serverId) throws IOException 
        {
            try (FileInputStream fileInputStream = new FileInputStream(file);
                DataOutputStream dataOutputStream = new DataOutputStream(secondarySocket.getOutputStream())) {
        
                dataOutputStream.writeUTF(file.getName());
                dataOutputStream.writeLong(file.length());
        
                byte[] buffer = new byte[1024];
                int bytesRead;
                long totalBytesSent = 0;
        
                while ((bytesRead = fileInputStream.read(buffer)) != -1) 
                {
                    dataOutputStream.write(buffer, 0, bytesRead);
                    totalBytesSent += bytesRead;
                }
                System.out.println("Total envoyé au serveur secondaire " + serverId + ": " + totalBytesSent + " bytes");
            }
        }

        // Les autres méthodes restent inchangées    
        private static void handleGet(DataInputStream clientInputStream, DataOutputStream clientOutputStream) throws IOException 
        {
            // Recevoir le nom du fichier demandé par le client
            String fileName = clientInputStream.readUTF();
            System.out.println("Commande GET pour le fichier : " + fileName);
        
            ByteArrayOutputStream completeFileData = new ByteArrayOutputStream(); // Stockage des données complètes du fichier
        
            // Connexion aux serveurs secondaires pour récupérer les parties
            for (int i = 1; i <= secondaryServerAddresses.size(); i++) 
            {
                String serverAddress = secondaryServerAddresses.get(i - 1);
                int serverPort = secondaryServerPorts.get(i - 1);
        
                try (Socket secondarySocket = new Socket(serverAddress, serverPort);
                    DataInputStream secondaryInputStream = new DataInputStream(secondarySocket.getInputStream());
                    DataOutputStream secondaryOutputStream = new DataOutputStream(secondarySocket.getOutputStream())) {
        
                    // Envoyer le nom de la partie demandée
                    String partName = "part_" + i + "_received_" + fileName; // Format des noms de parties
                    secondaryOutputStream.writeUTF("GET");
                    secondaryOutputStream.writeUTF(partName);
                    System.out.println("Demande envoyée au serveur secondaire pour : " + partName);
        
                    // Lire la taille de la partie
                    long fileSize = secondaryInputStream.readLong();
                    if (fileSize == -1) {
                        System.err.println("La partie " + i + " est introuvable sur le serveur secondaire.");
                        continue;
                    }
                    if (fileSize <= 0 || fileSize > Integer.MAX_VALUE) {
                        System.err.println("Erreur : Taille du fichier reçue invalide (" + fileSize + ").");
                        continue; // Passer à la partie suivante
                    }
        
                    System.out.println("Réception de la partie " + i + " de taille " + fileSize + " octets.");
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    long totalRead = 0;
        
                    while (totalRead < fileSize && (bytesRead = secondaryInputStream.read(buffer)) != -1) {
                        completeFileData.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
        
                    if (totalRead < fileSize) {
                        System.err.println("La partie " + i + " n'a pas été complètement reçue. Attendu : " + fileSize + " octets, Reçu : " + totalRead + " octets.");
                    }
                } catch (IOException e) {
                    System.err.println("Erreur lors de la récupération de la partie " + i + " : " + e.getMessage());
                }
            }
        
            // Vérifier si le fichier complet a été assemblé
            if (completeFileData.size() > 0) {
                byte[] fileData = completeFileData.toByteArray();
                clientOutputStream.writeUTF(fileName); // Envoyer le nom du fichier
                clientOutputStream.writeLong(fileData.length); // Envoyer la taille du fichier
                clientOutputStream.write(fileData); // Envoyer les données du fichier
                System.out.println("Fichier complet envoyé au client.");
            } else {
                clientOutputStream.writeUTF("Erreur : Impossible de récupérer le fichier " + fileName);
                System.err.println("Le fichier " + fileName + " n'a pas pu être récupéré.");
            }
        }

        private static void handleLs(DataOutputStream clientOutputStream) throws IOException 
        {
            // Liste des fichiers dans le serveur principal
            File directory = new File(serverPrincipalDirectory); 
            File[] files = directory.listFiles();

            if (files == null) {
                clientOutputStream.writeInt(0); // Aucun fichier trouvé
                System.out.println("Erreur : Le répertoire principal n'est pas accessible ou n'existe pas.");
                return;
            }

            if (files.length == 0) {
                clientOutputStream.writeInt(0); // Répertoire vide
                System.out.println("Le répertoire principal est vide.");
                return;
            }

            try {
                // Envoyer le nombre de fichiers
                clientOutputStream.writeInt(files.length);
                System.out.println("Nombre de fichiers trouvés : " + files.length);

                for (File file : files) 
                {
                    if (file.isFile()) 
                    {
                        System.out.println("Envoi du fichier : " + file.getName());
                        clientOutputStream.writeUTF(file.getName());
                    }
                }

                System.out.println("Liste des fichiers envoyée.");
            } catch (IOException e) {
                System.out.println("Erreur lors de l'envoi des fichiers : " + e.getMessage());
                e.printStackTrace();
            }

        
            // Liste des fichiers sur les serveurs secondaires
            // for (int i = 1; i <= secondaryServerAddresses.size(); i++) {
            //     String serverAddress = secondaryServerAddresses.get(i - 1);
            //     int serverPort = secondaryServerPorts.get(i - 1);
        
            //     try (Socket secondarySocket = new Socket(serverAddress, serverPort);
            //          DataInputStream secondaryInputStream = new DataInputStream(secondarySocket.getInputStream());
            //          DataOutputStream secondaryOutputStream = new DataOutputStream(secondarySocket.getOutputStream())) {
        
            //         // Envoyer la commande `ls` au serveur secondaire
            //         secondaryOutputStream.writeUTF("ls");
        
            //         // Recevoir la liste des fichiers
            //         int fileCount = secondaryInputStream.readInt();
            //         clientOutputStream.writeUTF("Fichiers sur le serveur secondaire " + i + " (" + fileCount + " fichiers) :");
        
            //         for (int j = 0; j < fileCount; j++) {
            //             String fileName = secondaryInputStream.readUTF();
            //             clientOutputStream.writeUTF(fileName);
            //         }
            //     } catch (IOException e) {
            //         clientOutputStream.writeUTF("Erreur : Serveur secondaire " + i + " inaccessible.");
            //         System.err.println("Erreur lors de la récupération des fichiers depuis le serveur secondaire " + i + " : " + e.getMessage());
            //     }
            // }
        }
        

        private static void handleRm(DataInputStream clientInputStream, DataOutputStream clientOutputStream) throws IOException 
        {
            String fileName = clientInputStream.readUTF();
            String fileName1 = "received_" + fileName;
            System.out.println("Commande RM pour le fichier : " + fileName1);

            boolean successOnPrimary = false;

            // Étape 1 : Suppression sur le serveur principal
            try 
            {
                File fileOnPrimary = new File(serverPrincipalDirectory, fileName1);
                if (fileOnPrimary.exists() && fileOnPrimary.delete()) 
                {
                    System.out.println("Fichier principal supprimé sur le serveur principal.");
                    clientOutputStream.writeUTF("Fichier supprimé avec succès sur le serveur principal.");
                    successOnPrimary = true;
                } 
                else {
                    System.err.println("Le fichier principal n'existe pas sur le serveur principal.");
                    clientOutputStream.writeUTF("Erreur : Le fichier principal n'existe pas sur le serveur principal.");
                }
            } catch (Exception e) {
                System.err.println("Erreur lors de la suppression sur le serveur principal : " + e.getMessage());
                clientOutputStream.writeUTF("Erreur : Impossible de supprimer le fichier principal sur le serveur principal.");
            }

            // Étape 2 : Suppression sur les serveurs secondaires
            for (int i = 0; i < secondaryServerAddresses.size(); i++) 
            {
                String serverAddress = secondaryServerAddresses.get(i);
                int serverPort = secondaryServerPorts.get(i);

                try (Socket secondarySocket = new Socket(serverAddress, serverPort);
                    DataOutputStream secondaryOutputStream = new DataOutputStream(secondarySocket.getOutputStream());
                    DataInputStream secondaryInputStream = new DataInputStream(secondarySocket.getInputStream())) {

                    // Suppression du fichier principal sur le serveur secondaire
                    secondaryOutputStream.writeUTF("RM");
                    secondaryOutputStream.writeUTF(fileName);

                    // Recevoir la réponse du serveur secondaire
                    String response = secondaryInputStream.readUTF();
                    System.out.println("Réponse des serveurs secondaires pour le fichier principal : " + response);
                    clientOutputStream.writeUTF("Serveur secondaire " + (i + 1) + " : " + response);

                    // Suppression des parties associées
                    boolean foundParts = false;
                    for (int partIndex = 1; partIndex <= secondaryServerAddresses.size()+1; partIndex++) // Pour les 3 parties
                    {
                        String partName = "part_" + partIndex + "_received_" + fileName;

                        // Envoyer la commande RM pour chaque partie
                        secondaryOutputStream.writeUTF("RM");
                        secondaryOutputStream.writeUTF(partName);

                        // Recevoir la réponse
                        response = secondaryInputStream.readUTF();
                        if ("NOT_FOUND".equals(response)) {
                            break; // Sortir de la boucle si aucune autre partie n'est trouvée
                        }

                        System.out.println("Réponse du serveur secondaire " + (i + 1) + " pour " + partName + " : " + response);
                        clientOutputStream.writeUTF("Serveur secondaire " + (i + 1) + " : " + response);
                        foundParts = true;
                    }

                    if (!foundParts) {
                        System.out.println("Aucune partie trouvée pour le fichier " + fileName + " sur le serveur secondaire " + (i + 1));
                        clientOutputStream.writeUTF("Aucune partie trouvée pour le fichier " + fileName + " sur le serveur secondaire " + (i + 1));
                    }

                    clientOutputStream.writeUTF("Les parties aussi ont ete effacees");

                } catch (IOException e) {
                    // System.err.println("Erreur lors de la suppression sur le serveur secondaire " + (i + 1) + " : " + e.getMessage());
                    clientOutputStream.writeUTF("Erreur : Serveur secondaire " + (i + 1) + " inaccessible.");
                }
            }

            // Étape 3 : Résumer le statut au client
            if (successOnPrimary) {
                clientOutputStream.writeUTF("La suppression a été effectuée sur le serveur principal et les serveurs secondaires.");
            } else {
                clientOutputStream.writeUTF("La suppression n'a pas pu être effectuée sur le serveur principal. Vérifiez les journaux.");
            }
        }

                
    }
}