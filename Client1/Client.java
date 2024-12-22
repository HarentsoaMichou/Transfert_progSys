package Client1;

import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.Scanner;

public class Client 
{

    private static int principalServerId;
    private static int principalServerPort;
    private static String principalServerAddress;
    private static String principalServerDirectory;
    private static int secondaryServerCount;
    private static int[] secondaryServerIds;
    private static int[] secondaryServerPorts;
    private static String[] secondaryServerAddresses;

    // Configuration du client
    private static String clientName;
    private static String clientVersion;
    private static int clientTimeout;
    private static String clientAddress;
    private static int clientPort;
    private static String clientDirectory;

    
    public static void main(String[] args) throws IOException 
    {
        if (args.length == 0) {
            System.err.println("Veuillez fournir un ID de client comme argument.");
            System.exit(1);
        }

        int clientID = Integer.parseInt(args[0]);

         // Charger la configuration pour ce client
         loadClientConfiguration(clientID);

         System.out.println("Client démarré : " + clientName);
         System.out.println("Connecté à " + clientAddress + ":" + clientPort);

        // Boucle principale pour permettre à l'utilisateur de choisir une action
        
        try (Scanner scanner = new Scanner(System.in)) 
        {
            while (true) 
            {
                System.out.println("Entrez une commande (PUT/GET/LS/RM/EXIT) :");
                String input = scanner.nextLine();
                String[] commandParts = input.split(" ", 2);
                String command = commandParts[0].toUpperCase();

                switch (command) {
                    case "PUT":
                        if (commandParts.length < 2) {
                            System.out.println("Veuillez spécifier le fichier à envoyer.");
                            continue;
                        }
                        sendFileToServer(commandParts[1]);
                        break;
                    case "GET":
                        if (commandParts.length < 2) {
                            System.out.println("Veuillez spécifier le fichier à récupérer.");
                            continue;
                        }
                        receiveFileFromServer(commandParts[1]);
                        break;
                    case "LS":
                        listFilesOnServer();
                        break;
                    case "RM":
                        if (commandParts.length < 2) {
                            System.out.println("Veuillez spécifier le fichier à supprimer.");
                            continue;
                        }
                        deleteFileFromServer(commandParts[1]);
                        break;
                    case "EXIT":
                        System.out.println("Fermeture du client.");
                        return;
                    default:
                        System.out.println("Commande inconnue. Essayez PUT, GET, LS, RM ou EXIT.");
                }
            }
        }
    }

    private static void loadClientConfiguration(int clientID) 
    {
        try (InputStream input = new FileInputStream("config.properties")) 
        {
            Properties prop = new Properties();
            prop.load(input);

            // Configuration du client
            clientName = prop.getProperty("client" + clientID + ".name", "UnknownClient");
            clientVersion = prop.getProperty("client" + clientID + ".version", "1.0");
            clientTimeout = Integer.parseInt(prop.getProperty("client" + clientID + ".timeout", "5000"));
            clientAddress = prop.getProperty("client" + clientID + ".address", "192.168.165.226");
            clientPort = Integer.parseInt(prop.getProperty("client" + clientID + ".port", "6789"));
            clientDirectory = prop.getProperty("client" + clientID + ".directory", "Client");

            // Configuration du serveur principal
            principalServerAddress = prop.getProperty("server.principal.address", "localhost");
            principalServerPort = Integer.parseInt(prop.getProperty("server.principal.port", "12345"));

            // Serveurs secondaires
            secondaryServerCount = Integer.parseInt(prop.getProperty("server.secondary.count", "0"));
            secondaryServerPorts = new int[secondaryServerCount];
            secondaryServerAddresses = new String[secondaryServerCount];
            for (int i = 0; i < secondaryServerCount; i++) {
                secondaryServerAddresses[i] = prop.getProperty("server.secondary" + (i + 1) + ".address", "localhost");
                secondaryServerPorts[i] = Integer.parseInt(prop.getProperty("server.secondary" + (i + 1) + ".port", "0"));
            }
        } catch (IOException ex) {
            System.err.println("Erreur lors du chargement de la configuration : " + ex.getMessage());
            System.exit(1);
        }
    }

    

    private static void sendFileToServer(String filePath) 
    {
        try (Socket socket = new Socket(principalServerAddress, principalServerPort); // Connexion au serveur principal
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {

            File file = new File(filePath);
            if (!file.exists()) {
                System.out.println("Le fichier spécifié n'existe pas.");
                return;
            }

            // Envoyer la commande PUT
            dataOutputStream.writeUTF("PUT");
            dataOutputStream.writeUTF(file.getName());
            dataOutputStream.writeLong(file.length());

            // Envoyer le contenu du fichier
            try (FileInputStream fileInputStream = new FileInputStream(file)) 
            {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                    dataOutputStream.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("Fichier " + file.getName() + " envoyé au serveur principal.");
        } catch (IOException e) {
            System.err.println("Erreur lors de l'envoi du fichier : " + e.getMessage());
        }
    }

    private static void receiveFileFromServer(String fileName) 
    {
        try (Socket socket = new Socket(principalServerAddress, principalServerPort); // Connexion au serveur principal
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream())) {

            // Envoyer la commande GET
            dataOutputStream.writeUTF("GET");
            dataOutputStream.writeUTF(fileName);

            // Recevoir la taille du fichier
            long fileSize = dataInputStream.readLong();
            if (fileSize == 0) {
                System.out.println("Le fichier " + fileName + " n'existe pas sur le serveur.");
                return;
            }

            // Recevoir le fichier
            File outputFile = new File(clientDirectory, "downloaded_" + fileName);
            try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) 
            {
                byte[] buffer = new byte[1024];
                int bytesRead;
                long totalRead = 0;

                while (totalRead < fileSize && (bytesRead = dataInputStream.read(buffer)) != -1) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                    totalRead += bytesRead;
                }
            }

            System.out.println("Fichier " + fileName + " téléchargé avec succès en tant que " + outputFile.getName());
        } catch (IOException e) {
            System.err.println("Erreur lors de la récupération du fichier : " + e.getMessage());
        }
    }

    private static void listFilesOnServer() 
    {
        try (Socket socket = new Socket(principalServerAddress, principalServerPort); // Connexion au serveur principal
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream())) {
    
            // Envoyer la commande LS
            dataOutputStream.writeUTF("LS");
            dataOutputStream.flush(); // S'assurer que la commande est envoyée immédiatement
    
            // Lire le nombre de fichiers
            int fileCount = dataInputStream.readInt();
            System.out.println("Nombre de fichiers sur le serveur : " + fileCount);
    
            if (fileCount == 0) {
                System.out.println("Aucun fichier sur le serveur.");
                return;
            }
    
            // Lire les noms des fichiers
            System.out.println("Fichiers sur le serveur principal :");
            for (int i = 0; i < fileCount; i++) 
            {
                String fileName = dataInputStream.readUTF(); // Lire chaque nom de fichier
                System.out.println("- " + fileName);
            }
    
        } catch (IOException e) {
            System.err.println("Erreur lors de la liste des fichiers : " + e.getMessage());
        }
    }
    

    private static void deleteFileFromServer(String fileName) 
    {
        try (Socket socket = new Socket(principalServerAddress, principalServerPort); // Connexion au serveur principal
             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream())) {

            // Envoyer la commande RM
            dataOutputStream.writeUTF("RM");
            dataOutputStream.writeUTF(fileName);

            // Recevoir la réponse du serveur (succès ou erreur)
            String response = dataInputStream.readUTF();
            System.out.println(response);
        } catch (IOException e) {
            System.err.println("Erreur lors de la suppression du fichier : " + e.getMessage());
        }
    }
}
