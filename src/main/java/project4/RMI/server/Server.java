package project4.RMI.server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of a Server class that represents a node in a Paxos distributed consensus system.
 * This server plays the role of Proposer, Acceptor, and Learner in the Paxos algorithm, and it also handles key-value store operations.
 */
public class Server extends UnicastRemoteObject implements ProposerInterface, AcceptorInterface, LearnerInterface, KVStoreInterface  {
  boolean isSuccess = false;
  double DIV = 2.0;
  int SERVER_DT=100;
  private final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();
  private final Map<String, Pair<String, Operation>> previousEntries;
  private AcceptorInterface[] acceptors;
  private LearnerInterface[] learners;
  private final int serverId;
  private final Map<String, Pair<Integer, Boolean>> learnerBook;
  private boolean serverStatus = false;
  private long serverDownTime = 0;


  /**
   * Constructor to create a Server instance.
   * @param serverId The unique ID of this server.
   */
  public Server(int serverId) throws RemoteException {
    this.serverId = serverId;
    this.previousEntries = new HashMap<>();
    this.learnerBook = new HashMap<>();
  }

  /**
   * Set the acceptors for this server.
   * @param acceptors Array of acceptors.
   */
  public void setAcceptors(AcceptorInterface[] acceptors) throws RemoteException {
    this.acceptors = acceptors;
  }


  /**
   * Set the learners for this server.
   * @param learners Array of learners.
   */
  public void setLearners(LearnerInterface[] learners) throws RemoteException {
    this.learners = learners;
  }

  /**
   * insert or update a value into the key-value store.
   * @param key key to be inserted.
   * @param value value to be inserted
   * @return response if the value is successfully updated
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  @Override
  public synchronized String put(String key, String value)
      throws RemoteException, InterruptedException {
    isSuccess = false;
    proposeOperation(new Operation("PUT", key, value));
    if (isSuccess)
      return "PUT operation successful for key - "+ key +" with value - "+value;
    else
      return "Error occurred during PUT operation for key - "+key;
  }

  /**
   * Delete a value from the key-value store.
   * @param key key to be deleted
   * @return response if the value is successfully deleted
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  @Override
  public synchronized String delete(String key) throws RemoteException, InterruptedException {
    isSuccess = false;
    proposeOperation(new Operation("DELETE", key, null));
    if (isSuccess)
      return "DELETE operation successful for key - "+ key;
    else
      return "Error occurred during DELETE operation for key - "+key;
  }

  /**
   * Get a value to a key from the key-value store.
   * @param key key to be inserted
   * @return value with respect to the key in the key-value store
   * @throws RemoteException if any issue in connecting to server
   */
  @Override
  public synchronized  String get(String key) throws RemoteException {
    if (keyValueStore.containsKey(key))
      return keyValueStore.get(key);
    return "No entry exist for they key - "+key;
  }

  @Override
  public Boolean containsKey(String key) throws RemoteException, InterruptedException {
    return keyValueStore.containsKey(key);
  }

  /**
   * Propose an operation to be applied.
   * @param operation The operation to be proposed.
   * @throws RemoteException If a remote error occurs.
   */
  private void proposeOperation(Operation operation) throws RemoteException, InterruptedException {
    String proposalId = generateProposalId();
    propose(proposalId, operation);
  }

  /**
   * Check if acceptor is down. Return a boolean value depending on the acceptor status.
   *
   * @return true if the acceptor is down
   */
  private boolean checkAcceptorStatus() throws RemoteException {
    if(serverStatus) {
      long currentTime = System.currentTimeMillis() / 1000L;
      if(this.serverDownTime + SERVER_DT <= currentTime) {
        serverStatus = false;
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Process the prepare operation of a acceptor. Receive the prepare request from the acceptor
   * and accept / reject it based on the if there's any latest operation in it's log.
   * @param proposalId The unique ID of the proposal.
   * @param oper operation to be performed
   * @return pair if  the given operation is accepted or not along with accepted operation
   * @throws RemoteException if there's any issue with RMI
   */
  @Override
  public synchronized Boolean prepare(String proposalId, Operation oper) throws RemoteException {
    if(checkAcceptorStatus()) {
      return null;
    }
    // check in the log for any highest value.
    if(this.previousEntries.containsKey(oper.key)) {
      if(Long.parseLong(this.previousEntries.get(oper.key).getKey().split(":")[1]) > Long.parseLong(proposalId.split(":")[1])) {
        return false;
      }
    }
    this.previousEntries.put(oper.key, new Pair<>(proposalId, oper));
    return true;
  }

  /**
   * Accept the value that the proposers give. If there's any operation with higher number, reject
   * the acceptance.
   * @param proposalId The unique ID of the proposal.
   * @param proposalValue The value of the proposal.
   * @throws RemoteException if issue arises with RMI
   */
  @Override
  public synchronized void accept(String proposalId, Operation proposalValue) throws RemoteException {
    if(checkAcceptorStatus()) {
      return;
    }
    // check in the log for any highest value.
    if(this.previousEntries.containsKey(proposalValue.key)) {
      if(Long.parseLong(this.previousEntries.get(proposalValue.key).getKey().split(":")[1]) <= Long.parseLong(proposalId.split(":")[1])) {
        for(LearnerInterface learner : this.learners) {
          learner.learn(proposalId, proposalValue);
        }
      }
    }
  }

  /**
   * Porpose a value to all the acceptors and get their input on the proposal. If all the
   * acceptors accept the proposal, send a accept request. If any of them rejects it, send a
   * accept request for the higher operation value.
   * @param proposalId The unique identifier for the proposal.
   * @param proposalValue The value being proposed.
   * @throws RemoteException if issue arises with RMI
   * @throws InterruptedException if sleep is interrupted
   */
  @Override
  public synchronized void propose(String proposalId, Operation proposalValue)
      throws RemoteException, InterruptedException {
    // Implement Paxos propose logic here
    List<Boolean> prepareResponse = new ArrayList<>();
    for(AcceptorInterface acceptor : this.acceptors) {
      Boolean res = acceptor.prepare(proposalId, proposalValue);
      prepareResponse.add(res);
    }
    int majorityCount = 0;

    // check for rejections and majority
    for(int i=0; i<5; i++) {
      if(prepareResponse.get(i) != null) {
        if(prepareResponse.get(i))
          majorityCount += 1;
      }
    }
    // if majority, accept the propsed value
    if(majorityCount >= Math.ceil(acceptors.length/DIV)) {
      for(int i=0; i<5; i++) {
        if(prepareResponse.get(i) != null)
          this.acceptors[i].accept(proposalId, proposalValue);
      }
    }
  }

  /**
   * learn the value that the acceptors pass.
   * @param proposalId The unique identifier for the proposal.
   * @param acceptedValue The value that has been accepted.
   * @throws RemoteException if any issue with the RMI
   */
  @Override
  public synchronized void learn(String proposalId, Operation acceptedValue) throws RemoteException {
    // Implement Paxos learn logic here
    if(!this.learnerBook.containsKey(proposalId)) {
      this.learnerBook.put(proposalId, new Pair<>(1, false));
    } else {
      Pair<Integer, Boolean> learnerPair = this.learnerBook.get(proposalId);
      learnerPair.setKey(learnerPair.getKey()+1);
      if(learnerPair.getKey() >= Math.ceil(acceptors.length/DIV) && !learnerPair.getValue()) {
        this.isSuccess = executeOperation(acceptedValue);
        learnerPair.setValue(true);
      }
      this.learnerBook.put(proposalId, learnerPair);
    }
  }

  /**
   * Generates a unique proposal ID.
   * @return A unique proposal ID.
   */
  private String generateProposalId() throws RemoteException {
    // Placeholder code to generate a unique proposal ID
    return serverId + ":" + System.currentTimeMillis();
  }

  /**
   * Apply the given operation to the key-value store.
   * @param operation The operation to apply.
   */
  private boolean executeOperation(Operation operation) throws RemoteException {
    if (operation == null) return false;
    switch (operation.type) {
      case "PUT":
        keyValueStore.put(operation.key, operation.value);
        System.out.println(System.currentTimeMillis()+" - PUT Operation successfull for Key:Value - " + operation.key + ":" + operation.value);
        return true;
      case "DELETE":
        if(keyValueStore.containsKey(operation.key)) {
          keyValueStore.remove(operation.key);
          System.out.println(System.currentTimeMillis()+" - DELETE Operation successful for Key - " + operation.key );
          return true;
        } else {
          System.out.println(System.currentTimeMillis()+" - DELETE Operation Failed for Key - " + operation.key );
          return false;
        }
      default:
        throw new IllegalArgumentException("Unknown operation type: " + operation.type);
    }
  }
  /**
   * Set an Server as down
   */
  public void setServerDown() {
    this.serverStatus = true;
    this.serverDownTime = System.currentTimeMillis() / 1000L;
  }

}

/**
 * class representing an operation on the key-value store.
 */
class Operation {
  String type;
  String key;
  String value;

  Operation(String type, String key, String value) {
    this.type = type;
    this.key = key;
    this.value = value;
  }

}

/**
 * Create a Pair Object with any two generic types.
 * @param <K> Generic K that is used in pair creation
 * @param <T> Generic T that is used in pair creation
 */
class Pair<K, T> {
  private T value;
  private K key;

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  Pair(K key, T value) {
    this.key = key;
    this.value = value;
  }

}
