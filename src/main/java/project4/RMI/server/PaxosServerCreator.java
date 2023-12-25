package project4.RMI.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The PaxosServerCreator class is responsible for creating and binding the Paxos servers
 * within the RMI registry. It also configures the acceptors and learners for each server.
 */
public class PaxosServerCreator {

  /**
   * Create a scheduler that drops servers at random time.
   * @param servers list of servers to be dropped
   */
  private static void scheduler(Server[] servers) {
    // Schedule a task to run at a random delay, then repeat every X seconds
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        dropServer(servers);
      }
    }, 10000, 100000);
  }

  /**
   * drop a server at random or just ignore when triggered.
   * @param servers list of servers to be dropped
   */
  private static void dropServer(Server[] servers)  {
    int id = (int) (Math.random() * servers.length);
    servers[id].setServerDown();
    System.out.println(System.currentTimeMillis() + " -- Server " + id + " is going down");
  }

  /**
   * The main method to launch the creation and binding process of the Paxos servers.
   *
   * @param args Command-line arguments (unused in this context).
   */
  public static void main(String[] args) {
    try {
      int serversNum = 5;
      try {
        // Check for correct number of command-line arguments
        if (args.length != 2) {
          System.out.println("Time : " + System.currentTimeMillis() + " - Usage: java PaxosServer c");
          System.exit(1);
        }

        // Extract command-line arguments
        int portInput = Integer.parseInt(args[0]);
        String remoteObjectName = args[1];

        Server[] servers = new Server[serversNum];

        // Create and bind servers
        for (int serverId = 0; serverId < serversNum; serverId++) {
          int port = portInput + serverId; // Increment port for each server
          // Create RMI registry at the specified port
          LocateRegistry.createRegistry(port);

          // Create server instance
          servers[serverId] = new Server(serverId);

          // Bind the server to the RMI registry
          Registry registry = LocateRegistry.getRegistry(port);
          registry.rebind(remoteObjectName, servers[serverId]);

          System.out.println("Server " + serverId + " is ready at port " + port);
        }
        scheduler(servers);
        // Set acceptors and learners for each server
        for (int serverId = 0; serverId < serversNum; serverId++) {
          AcceptorInterface[] acceptors = new AcceptorInterface[serversNum];
          LearnerInterface[] learners = new LearnerInterface[serversNum];
          for (int i = 0; i < serversNum; i++) {
            acceptors[i] = servers[i];
            learners[i] = servers[i];
          }
          servers[serverId].setAcceptors(acceptors);
          servers[serverId].setLearners(learners);
        }

      } catch (Exception e) {
        System.err.println("Server exception: " + e.toString());
        e.printStackTrace();
      }
    } catch (Exception e) {
      System.out.println(
          "Time : " + System.currentTimeMillis() + " - Exception occurred while processing project2.RMI client with message" +
              e.getMessage());
    }
  }
}
