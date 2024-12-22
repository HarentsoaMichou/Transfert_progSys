package ServeurSecondaire1;

import java.io.*;
import java.net.*;
import java.util.*;

public class ServeurSecondaire 
{

    private static int serverId;
    private static int basePort;
    private static String directoryPath;
    private static String serverPrincipalPath;
    private static List<String> secondaryServerDirectories = new ArrayList<>();

    // Lecture de la configuration à partir du fichier config.properties
    public static void loadConfiguration() 
    {
        try (InputStream input = new FileInputStream("config.properties")) 
        {
            Properties prop = new Properties();
            prop.load(input);

            // Charger la configuration spécifique à ce serveur secondaire
            basePort = Integer.parseInt(prop.getProperty("server.secondary" + serverId + ".port.base"));
            directoryPath = prop.getProperty("server.secondary" + serverId + ".directory", "ServeurSecondaire1");

            serverPrincipalPath = prop.getProperty("server.principal.directory", "ServeurPrincipal");

            int numberOfSecondaryServers = Integer.parseInt(prop.getProperty("server.secondary.count", "2"));
            for (int i = 1; i <= numberOfSecondaryServers; i++) 
            {
                String secondaryDirectory = prop.getProperty("server.secondary" + i + ".directory", "ServeurSecondaire");
                secondaryServerDirectories.add(secondaryDirectory);
            }

        } catch (IOException ex) {
            System.err.println("Erreur lors du chargement de la configuration : " + ex.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException 
    {
        if (args.length < 1) {
            System.err.println("Veuillez spécifier l'ID du serveur secondaire (ex : 1, 2, etc.).");
            System.exit(1);
        }

        // Récupérer l'ID du serveur à partir des arguments de la ligne de commande
        serverId = Integer.parseInt(args[0]);

        // Charger la configuration après avoir déterminé l'ID du serveur
        loadConfiguration();

        int port = basePort + serverId; // Port unique pour chaque serveur secondaire

        // Création du socket serveur
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Serveur secondaire " + serverId + " prêt à recevoir des commandes sur le port " + port + "...");

        while (true) 
        {
            Socket socket = serverSocket.accept();
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

            try {
                // Lire la commande (PUT pour recevoir un fichier ou GET pour envoyer une partie)
                String command = dataInputStream.readUTF();
                System.out.println("Commande reçue : " + command);

                // Gestion des commandes avec switch
                switch (command.toUpperCase()) {
                    case "PUT":
                        // Gérer la réception d'un fichier
                        receiveFile(dataInputStream);
                        break;

                    case "GET":
                        // Gérer l'envoi d'une partie
                        sendFilePart(dataInputStream, dataOutputStream);
                        break;

                    case "LS":
                        // Gérer la liste des fichiers
                        handleListFiles(dataInputStream, dataOutputStream);
                        break;

                    case "RM":
                        // Gérer la suppression d'un fichier
                        handleRemoveFile(dataInputStream, dataOutputStream);
                        break;

                    default:
                        // System.out.println("Commande inconnue : " + command);
                        dataOutputStream.writeUTF("Erreur : Commande inconnue.");
                        break;
                }
            } catch (IOException e) {
                System.err.println("Erreur lors du traitement : " + e.getMessage());
            } finally {
                dataInputStream.close();
                dataOutputStream.close();
                socket.close();
            }
        }
    }

    // Méthode pour recevoir un fichier (commande PUT)
    private static void receiveFile(DataInputStream dataInputStream) throws IOException 
    {
        String fileName = dataInputStream.readUTF();
        long fileSize = dataInputStream.readLong();

        File file = new File(directoryPath, "received_" + fileName);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = dataInputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }

        System.out.println("Fichier " + fileName + " reçu avec succès.");
    }

    // Méthode pour envoyer une partie de fichier (commande GET)
    public static void sendFilePart(DataInputStream inputStream, DataOutputStream outputStream) 
    {
        try {
            // Recevoir la demande (nom du fichier demandé)
            String requestedFileName = inputStream.readUTF();
            System.out.println("Demande reçue pour le fichier : " + requestedFileName);

            // Vérifier si le fichier existe dans le répertoire spécifié
            File filePart = new File(directoryPath, requestedFileName);
            System.out.println("Chemin complet du fichier : " + filePart.getAbsolutePath());

            if (!filePart.exists() || !filePart.isFile()) {
                System.err.println("Erreur : le fichier " + filePart.getName() + " n'existe pas ou est invalide.");
                outputStream.writeLong(-1); // Taille invalide pour signaler une erreur
                return;
            }

            // Envoyer la taille du fichier
            long fileSize = filePart.length();
            System.out.println("Envoi de la taille du fichier : " + fileSize);
            outputStream.writeLong(fileSize);

            // Envoyer le contenu du fichier
            try (FileInputStream fileInputStream = new FileInputStream(filePart)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("Fichier " + filePart.getName() + " envoyé avec succès.");
        } catch (IOException e) {
            System.err.println("Erreur lors de l'envoi de la partie : " + e.getMessage());
        }
    }

    private static void handleListFiles(DataInputStream inputStream, DataOutputStream outputStream) throws IOException 
    {
        File directory = new File(directoryPath); // Répertoire spécifié dans le fichier de configuration
        File[] files = directory.listFiles();

        if (files == null) {
            outputStream.writeInt(0); // Aucun fichier trouvé
            System.out.println("Aucun fichier trouvé.");
            return;
        }

        // Envoyer le nombre de fichiers
        outputStream.writeInt(files.length);

        for (File file : files) 
        {
            if (file.isFile()) {
                outputStream.writeUTF(file.getName());
            }
        }

        System.out.println("Liste des fichiers envoyée.");
    }

    // private static void handleRemoveFile(DataInputStream inputStream, DataOutputStream outputStream) throws IOException 
    // {
    //     String fileName = inputStream.readUTF();
    //     File file = new File(directoryPath, fileName);

    //     if (file.exists() && file.isFile()) {
    //         if (file.delete()) {
    //             outputStream.writeUTF("Fichier supprimé : " + fileName);
    //             System.out.println("Fichier " + fileName + " supprimé.");
    //         } else {
    //             outputStream.writeUTF("Erreur : Impossible de supprimer le fichier " + fileName);
    //         }
    //     } else {
    //         outputStream.writeUTF("Erreur : Fichier introuvable : " + fileName);
    //     }
    // }


    private static void handleRemoveFile(DataInputStream inputStream, DataOutputStream outputStream) throws IOException 
    {
        String fileName = inputStream.readUTF();
        String fileName1 = "received_" + fileName;

        // Étape 1 : Supprimer le fichier principal
        File principalFile = new File(serverPrincipalPath, fileName1);
        boolean deletionSuccess = false;

        if (principalFile.exists() && principalFile.isFile()) 
        {
            if (principalFile.delete()) {
                System.out.println("Fichier principal supprimé : " + fileName1);
                deletionSuccess = true;
            } else {
                outputStream.writeUTF("Erreur : Impossible de supprimer le fichier principal : " + fileName1);
                return;
            }
        } 

        // Étape 2 : Supprimer les fichiers partiels associés dans plusieurs répertoires
        boolean foundParts = false;

        for (String dirPath : secondaryServerDirectories) 
        {
            File directory = new File(dirPath);
            if (!directory.exists() || !directory.isDirectory()) {
                System.err.println("Erreur : Répertoire introuvable ou inaccessible : " + dirPath);
                continue;
            }

            File[] files = directory.listFiles();
            if (files == null) {
                System.err.println("Erreur : Impossible de lister les fichiers dans le répertoire : " + dirPath);
                continue;
            }

            for (File file : files) 
            {
                if (file.getName().startsWith("part_") && file.getName().endsWith("_received_" + fileName)) {
                    foundParts = true;
                    if (file.delete()) {
                        // System.out.println("Fichier partiel supprimé dans " + dirPath + " : " + file.getName());
                        outputStream.writeUTF("Les parties du fichier '" + fileName + "' ont ete supprimees");
                    } else {
                        System.err.println("Erreur : Impossible de supprimer le fichier partiel dans " + dirPath + " : " + file.getName());
                    }
                }
            }
        }

        // Étape 3 : Retourner une réponse au client
        if (foundParts) {
            outputStream.writeUTF("Fichiers supprimés avec succès : " + fileName + " et ses parties associées.");
        } else if (deletionSuccess) {
            outputStream.writeUTF("Fichier principal supprimé, mais aucune partie associée trouvée.");
        } else {
            outputStream.writeUTF("Erreur : Aucun fichier ou partie associée trouvé pour : " + fileName);
        }
    }

    
}
