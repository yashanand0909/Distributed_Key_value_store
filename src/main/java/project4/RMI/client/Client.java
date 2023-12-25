package project4.RMI.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.ServerNotActiveException;
import java.util.Random;
import java.util.Scanner;
import project4.RMI.common.ProcessRequest;
import project4.RMI.server.KVStoreInterface;

/**
 * This class represents the client for a remote method invocation (RMI) based key-value store system.
 * The client interacts with a remote server that implements the RemoteInterface to perform various
 * operations on the key-value store.
 */
public class Client {

  /**
   * The main method to start the RMI client.
   * @param args Command-line arguments: [hostname] [port] [remoteObjectName]
   */
  public static void main(String[] args) {
    try {
      // Check for correct number of command-line arguments
      if (args.length != 3) {
        System.out.println("Time : " + System.currentTimeMillis() + " - Usage: java PaxosClient c");
        System.exit(1);
      }

      // Extract command-line arguments
      String hostname = args[0];
      int port = Integer.parseInt(args[1]);
      String remoteObjectName = args[2];

      // Custom RMIClientSocketFactory for creating client sockets
      RMIClientSocketFactory csf = new RMIClientSocketFactory() {
        /**
         * Creates a socket with a timeout of 5 seconds for connection.
         * @param host The remote host address
         * @param port The remote host port
         * @return The socket created for communication
         * @throws IOException If an I/O error occurs during socket creation
         */
        public Socket createSocket(String host, int port) throws IOException {
          Socket socket = new Socket();
          socket.connect(new InetSocketAddress(host, port), 5000); // 5 sec timeout
          return socket;
        }

        /**
         * Creates a server socket for RMI communication.
         * @param port The port to bind the server socket
         * @return The server socket created
         * @throws IOException If an I/O error occurs during server socket creation
         */
        public ServerSocket createServerSocket(int port) throws IOException {
          return new ServerSocket(port);
        }
      };

      csf.createSocket(hostname, port);

      // Obtain a reference to the remote object from the RMI registry
      Random random = new Random();
      int addition = random.nextInt(4);
      Registry registry = LocateRegistry.getRegistry(hostname, port + addition, csf);
      KVStoreInterface remoteObject = (KVStoreInterface) registry.lookup(remoteObjectName);

      // Perform pre-population of the key-value store
      for (int i = 0; i < 10; i++) {
        System.out.println("Time : " + System.currentTimeMillis() + " Starting pre-Population of key value store");
        handleOperation("PUT key" + i + " value" + i, remoteObject);
        System.out.println("Time : " + System.currentTimeMillis() + " Pre-Population of key value store complete");
      }

      // Perform GET operations on the key-value store
      for (int i = 0; i < 5; i++) {
        System.out.println("Time : " + System.currentTimeMillis() + " Starting GET Operation");
        handleOperation("GET key" + i, remoteObject);
        System.out.println("Time : " + System.currentTimeMillis() + " GET Operation complete");
      }

      // Perform DELETE operations on the key-value store
      for (int i = 0; i < 5; i++) {
        System.out.println("Time : " + System.currentTimeMillis() + " Starting DELETE Operation");
        handleOperation("DELETE key" + i, remoteObject);
        System.out.println("Time : " + System.currentTimeMillis() + " DELETE Operation complete");
      }

      // Perform PUT operations on the key-value store
      for (int i = 5; i < 10; i++) {
        System.out.println("Time : " + System.currentTimeMillis() + " Starting PUT Operation");
        handleOperation("PUT key" + i + " value" + i, remoteObject);
        System.out.println("Time : " + System.currentTimeMillis() + " PUT Operation complete");
      }

      // Interactive loop to handle user input for operations
      while (true) {
        try {
          Scanner sc = new Scanner(System.in);
          System.out.println("Time : " + System.currentTimeMillis() + " - Enter the operation string or enter EXIT to exit the client: ");
          String operation = sc.nextLine();
          addition = random.nextInt(4);
          registry = LocateRegistry.getRegistry(hostname, port + addition, csf);
          System.out.println("Server port - " + port + addition);
          remoteObject = (KVStoreInterface) registry.lookup(remoteObjectName);
          if (operation.equalsIgnoreCase("EXIT"))
            break;
          else if (operation.startsWith("PUT ") || operation.startsWith("GET ") || operation.startsWith("DELETE ")) {
            handleOperation(operation, remoteObject);
          }
        } catch (RemoteException e) {
          System.out.println("Time : " + System.currentTimeMillis() + " - RemoteException occurred while processing project2.RMI client request");
        } catch (ServerNotActiveException se) {
          System.out.println("Time : " + System.currentTimeMillis() + " - ServerNotActiveException occurred while processing project2.RMI client request");
        } catch (Exception e) {
          System.out.println("Time : " + System.currentTimeMillis() + " - Exception occurred while processing project2.RMI client request with message" + e.getMessage());
        }
      }
    } catch (RemoteException e) {
      System.out.println("Time : " + System.currentTimeMillis() + " - RemoteException occurred while processing project2.RMI client registry");
    } catch (Exception e) {
      System.out.println("Time : " + System.currentTimeMillis() + " - Exception occurred while processing project2.RMI client with message" + e.getMessage());
    }
  }

  /**
   * Handles the specified operation on the key-value store by invoking the corresponding method on the remote object.
   *
   * @param operation    The operation to be performed on the key-value store (e.g., "PUT key value", "GET key", "DELETE key").
   * @param remoteObject The reference to the remote object implementing the RemoteInterface.
   * @throws ServerNotActiveException If the server is not active during the RMI call.
   * @throws RemoteException          If an RMI communication-related exception occurs.
   */
  private static void handleOperation(String operation, KVStoreInterface remoteObject)
      throws ServerNotActiveException, RemoteException, InterruptedException {
    System.out.println("Time : " + System.currentTimeMillis() + " Received operation - " + operation);
    ProcessRequest response = processRequest(operation, remoteObject);
    String responseData;
    if (!response.status) {
      System.out.println("Time : " + System.currentTimeMillis() + " - Received malformed request of length " + operation.length());
      responseData = response.message;
    } else {
      responseData = response.value;
    }
    System.out.println("Time : " + System.currentTimeMillis() + " Response from server - " + responseData);
  }

  /**
   * Processes the specified request by parsing the operation and invoking the corresponding method on the remote object.
   *
   * @param requestData  The request data containing the operation (e.g., "PUT key value", "GET key", "DELETE key").
   * @param remoteObject The reference to the remote object implementing the RemoteInterface.
   * @return A ProcessRequest object containing the response status and message.
   * @throws RemoteException          If an RMI communication-related exception occurs.
   * @throws ServerNotActiveException If the server is not active during the RMI call.
   */
  private static ProcessRequest processRequest(String requestData, KVStoreInterface remoteObject)
      throws RemoteException, InterruptedException {

    // Example: PUT (key, value)
    if (requestData.startsWith("PUT")) {
      String[] parts = requestData.split(" ");
      if (parts.length == 3) {
        String key = parts[1];
        String value = parts[2];
        remoteObject.put(key, value);
        return new ProcessRequest(true, "PUT process successful", "Key:" + key + " added with the Value:" + value);
      } else {
        return new ProcessRequest(false, "PUT operation failed due to malformed input", "");
      }
    }

    // Example: GET (key)
    if (requestData.startsWith("GET")) {
      String[] parts = requestData.split(" ");
      if (parts.length == 2) {
        String key = parts[1];
        if (remoteObject.containsKey(key)) {
          String value = remoteObject.get(key);
          return new ProcessRequest(true, "GET process successful", "Value returned for the given Key is : " + value);
        } else {
          return new ProcessRequest(false, "Key not found in key store", "");
        }
      } else {
        return new ProcessRequest(false, "GET operation failed due to malformed input", "");
      }
    }

    // Example: DELETE (key)
    if (requestData.startsWith("DELETE")) {
      String[] parts = requestData.split(" ");
      if (parts.length == 2) {
        String key = parts[1];
        remoteObject.delete(key);
        return new ProcessRequest(true, "DELETE process successful", "Value deleted for Key:" + key);
      } else {
        return new ProcessRequest(false, "DELETE operation failed due to malformed input", "");
      }
    }
    return new ProcessRequest(false, "Operation failed due to malformed input", "");
  }
}
