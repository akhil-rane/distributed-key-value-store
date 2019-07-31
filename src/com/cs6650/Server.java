package com.cs6650;

import java.net.*;
import java.nio.file.Files;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.*; 
import com.cs6650.Message.MessageContent;
import java.util.concurrent.TimeUnit;

public class Server extends UnicastRemoteObject implements DatastoreInterface
{ 

	private static final long serialVersionUID = 1L;

	private String serverID;

	// Stores all the data sent from the client
	private HashMap<String,String> storage = new HashMap<String, String>();

	private HashMap<String,String> undoStorageLog = new HashMap<String, String>();
	private File undoLog;
	private HashMap<String,String> redoStorageLog = new HashMap<String, String>();
	private File redoLog;
	

	// Server log is stored in logs folder
	public Logger logger;

	private Registry registry;

	private int port;

	private boolean isMaster = true;

	private String master_address;

	private int master_port;


	protected Server(String serverID, Registry registry, int port) throws RemoteException {
		super();
		this.serverID = serverID;
		this.registry = registry;
		this.logger = getLogger("logs/"+serverID+"_server.log");
		this.port = port;
		
		this.undoLog = new File("logs/undo_"+serverID+"_storage.log");
		this.redoLog = new File("logs/redo_"+serverID+"_storage.log");

		try {
			if(undoLog.exists()) {
				logger.info("Fetching undo log");
				ObjectInputStream oisUndo = new ObjectInputStream(new FileInputStream(undoLog));
				this.undoStorageLog = (HashMap<String, String>) oisUndo.readObject();
				oisUndo.close();
				logger.info("Successfully fetched undo log");
			}

			if(redoLog.exists()) {
				logger.info("Fetching redo log");
				ObjectInputStream oisRedo = new ObjectInputStream(new FileInputStream(redoLog));
				this.undoStorageLog = (HashMap<String, String>) oisRedo.readObject();
				oisRedo.close();
				logger.info("Successfully fetched redo log");
			}
		}
		catch(Exception e) {
			System.err.println("Server Initialization failed: " + e.toString());
			logger.log(Level.SEVERE, "Server Initialization failed", e);
			System.exit(0);
		}
	}
	
	public void registerNewServer(String currentServerID, DatastoreInterface server) throws RemoteException, AlreadyBoundException{
		this.registry.bind(currentServerID, server);
		this.logger.info("Registered new server: "+currentServerID);
	}
	
	public String getServerID() throws RemoteException{
		return serverID;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}

	public String getMaster_address() {
		return master_address;
	}

	public void setMaster_address(String master_address) {
		this.master_address = master_address;
	}

	public int getMaster_port() {
		return master_port;
	}

	public void setMaster_port(int master_port) {
		this.master_port = master_port;
	}

	public boolean isMaster() {
		return isMaster;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}

	public HashMap<String, String> getStorage() throws RemoteException {
		return storage;
	}

	public void setStorage(HashMap<String, String> storage) {
		this.storage = storage;
	}

	public synchronized Response put(String key, String value) throws RemoteException {
		logger.info("Request Query [type=" + "put" + ", key=" + key + ", value=" + value + "]");
		Response response = new Response();

		Transaction transaction = new Transaction();
		transaction.setType("put");
		transaction.setKey(key);
		transaction.setValue(value);

		Message queryToConflict = new Message();
		queryToConflict.setContent(MessageContent.QUERY_TO_COMMIT);
		queryToConflict.setTransaction(transaction);

		List<DatastoreInterface> servers = getServers();
		logger.info("Fetched all the cohorts/participants to commit the change");
		
		boolean queryToConflictFailed = false;
		
		for(DatastoreInterface server: servers) {
			try {
				logger.info("Query to commit on server: "+server.getServerID());
				Message resp = server.send(queryToConflict);
				if (resp.content == MessageContent.NO) {
					logger.info("Query to commit failed on server: "+server.getServerID());
					queryToConflictFailed = true;
					break;
				}
			}
			catch(Abort e) {
				logger.info("Query to commit failed on server: "+server.getServerID());
				queryToConflictFailed = true;
				break;
			}
			catch(Exception e) {
				logger.info("Query to commit failed as Remote server is down, removing it from accessible servers list");
				servers.remove(server);
				queryToConflictFailed = true;
				break;
			}
		}
		
		if(queryToConflictFailed) {
			logger.info("Initializing rollback procedure");
			Message rollback = new Message();
			rollback.setContent(MessageContent.ROLLBACK);
			
			for(DatastoreInterface server: servers) {
				boolean rollbackFailed = true;
				while(rollbackFailed) {
					try {
						logger.info("Rollback on server: "+server.getServerID());
						Message resp = server.send(rollback);
						if (resp.content == MessageContent.NO) {
							logger.info("Rollback failed on server: "+server.getServerID());
							rollbackFailed = true;
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting rollback on server: "+server.getServerID());
							continue;
						}
					}
					catch(Abort e) {
						logger.info("Rollback failed on server: "+server.getServerID());
						rollbackFailed = true;
						try {
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting rollback on server: "+server.getServerID());
							continue;
						} catch (InterruptedException e1) {
							System.err.println("System failed in undesirable state: " + e1.toString());
							logger.log(Level.SEVERE, "System failed in undesirable state", e1);
							System.exit(0);
						}
					} catch (InterruptedException e) {
						System.err.println("System failed in undesirable state: " + e.toString());
						logger.log(Level.SEVERE, "System failed in undesirable state", e);
						System.exit(0);
					}
					catch(Exception e) {
						logger.info("Rollback failed as Remote server is down, removing it from accessible servers list");
						servers.remove(server);
						rollbackFailed = true;
						break;
					}
					
					logger.info("Rollback succeeded on server: "+server.getServerID());
					rollbackFailed = false;
				}
			}
			response.setType("put");
			response.setReturnValue(null);
			response.setMessage("Failed to insert the entry in storage");
		}
		else {
			logger.info("Initializing commit procedure");
			Message commit = new Message();
			commit.setContent(MessageContent.COMMIT);
			
			for(DatastoreInterface server: servers) {
				boolean commitFailed = true;
				while(commitFailed) {
					try {
						logger.info("Commit on server: "+server.getServerID());
						Message resp = server.send(commit);
						if (resp.content == MessageContent.NO) {
							logger.info("Commit failed on server: "+server.getServerID());
							commitFailed = true;
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting commit on server: "+server.getServerID());
							continue;
						}
					}
					catch(Abort e) {
						logger.info("Commit failed on server: "+server.getServerID());
						commitFailed = true;
						try {
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting commit on server: "+server.getServerID());
							continue;
						} catch (InterruptedException e1) {
							System.err.println("System failed in undesirable state: " + e1.toString());
							logger.log(Level.SEVERE, "System failed in undesirable state", e1);
							System.exit(0);
						}
					} catch (InterruptedException e) {
						System.err.println("System failed in undesirable state: " + e.toString());
						logger.log(Level.SEVERE, "System failed in undesirable state", e);
						System.exit(0);
					}
					catch(Exception e) {
						logger.info("Commit failed as Remote server is down, removing it from accessible servers list");
						servers.remove(server);
						commitFailed = true;
						break;
					}
					
					logger.info("Commit succeeded on server: "+server.getServerID());
					commitFailed = false;
				}
			}
			
			undoStorageLog = (HashMap<String,String>)storage.clone();
			ObjectOutputStream outputUndo;
			try {
				outputUndo = new ObjectOutputStream(new FileOutputStream(this.undoLog));
				outputUndo.writeObject(undoStorageLog);
				outputUndo.flush();
				outputUndo.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			storage.put(key,value);
			
			redoStorageLog = (HashMap<String,String>)storage.clone();
			ObjectOutputStream outputredo;
			try {
				outputredo = new ObjectOutputStream(new FileOutputStream(this.redoLog));
				outputredo.writeObject(redoStorageLog);
				outputredo.flush();
				outputredo.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			logger.info("Successfully completed transaction on coordinator: "+this.getServerID());
			
			response.setType("put");
			response.setReturnValue(null);
			response.setMessage("Successfully inserted the entry in storage");
		}

		logger.info(response.toString());
		return response;
	}
	
	public synchronized Response preparePut(String key, String value) throws RemoteException {
		logger.info("Request Query [type=" + "put" + ", key=" + key + ", value=" + value + "]");
		
		undoStorageLog = (HashMap<String,String>)storage.clone();
		ObjectOutputStream outputUndo;
		try {
			outputUndo = new ObjectOutputStream(new FileOutputStream(this.undoLog));
			outputUndo.writeObject(undoStorageLog);
			outputUndo.flush();
			outputUndo.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		storage.put(key,value);
		
		redoStorageLog = (HashMap<String,String>)storage.clone();
		ObjectOutputStream outputredo;
		try {
			outputredo = new ObjectOutputStream(new FileOutputStream(this.redoLog));
			outputredo.writeObject(redoStorageLog);
			outputredo.flush();
			outputredo.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Response response = new Response();
		response.setType("put");
		response.setReturnValue(null);
		response.setMessage("successfully inserted entry in storage");
		
		logger.info(response.toString());
		return response;
	}
	

	public synchronized Response prepareDelete(String key) throws RemoteException {
		logger.info("Request Query [type=" + "delete" + ", key=" + key + "]");
		
		undoStorageLog = (HashMap<String,String>)storage.clone();
		ObjectOutputStream outputUndo;
		try {
			outputUndo = new ObjectOutputStream(new FileOutputStream(this.undoLog));
			outputUndo.writeObject(undoStorageLog);
			outputUndo.flush();
			outputUndo.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		storage.remove(key);
		
		redoStorageLog = (HashMap<String,String>)storage.clone();
		ObjectOutputStream outputredo;
		try {
			outputredo = new ObjectOutputStream(new FileOutputStream(this.redoLog));
			outputredo.writeObject(redoStorageLog);
			outputredo.flush();
			outputredo.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Response response = new Response();
		response.setType("delete");
		response.setReturnValue(null);
		response.setMessage("successfully deleted entry from storage");
		
		logger.info(response.toString());
		return response;
	}


	public synchronized Response get(String key) throws RemoteException {
		logger.info("Request Query [type=" + "get" + ", key=" + key + "]");

		Response response = new Response();
		response.setType("get");

		if(!storage.containsKey(key)){
			response.setReturnValue(null);
			response.setMessage("key "+key+" does not exist in the storage");
		}
		else {
			String val = storage.get(key);
			response.setReturnValue(val);
			response.setMessage("successfully retrieved entry from storage");
		}	

		logger.info(response.toString());
		return response;
	}


	public synchronized Response delete(String key) throws RemoteException {
		logger.info("Request Query [type=" + "delete" + ", key=" + key + "]");
		Response response = new Response();

		Transaction transaction = new Transaction();
		transaction.setType("delete");
		transaction.setKey(key);
		transaction.setValue(null);

		Message queryToConflict = new Message();
		queryToConflict.setContent(MessageContent.QUERY_TO_COMMIT);
		queryToConflict.setTransaction(transaction);

		List<DatastoreInterface> servers = getServers();
		logger.info("Fetched all the cohorts/participants to commit the change");
		
		boolean queryToConflictFailed = false;

		for(DatastoreInterface server: servers) {
			try {
				logger.info("Query to commit on server: "+server.getServerID());
				Message resp = server.send(queryToConflict);
				if (resp.content == MessageContent.NO) {
					logger.info("Query to commit failed on server: "+server.getServerID());
					queryToConflictFailed = true;
					break;
				}
			}
			catch(Abort e) {
				logger.info("Query to commit failed on server: "+server.getServerID());
				queryToConflictFailed = true;
				break;
			}
			catch(Exception e) {
				logger.info("Query to commit failed as Remote server is down, removing it from accessible servers list");
				servers.remove(server);
				queryToConflictFailed = true;
				break;
			}
		}

		if(queryToConflictFailed) {
			logger.info("Initializing rollback procedure");
			Message rollback = new Message();
			rollback.setContent(MessageContent.ROLLBACK);
			
			for(DatastoreInterface server: servers) {
				boolean rollbackFailed = true;
				while(rollbackFailed) {
					try {
						logger.info("Rollback on server: "+server.getServerID());
						Message resp = server.send(rollback);
						if (resp.content == MessageContent.NO) {
							logger.info("Rollback failed on server: "+server.getServerID());
							rollbackFailed = true;
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting rollback on server: "+server.getServerID());
							continue;
						}
					}
					catch(Abort e) {
						logger.info("Rollback failed on server: "+server.getServerID());
						rollbackFailed = true;
						try {
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting rollback on server: "+server.getServerID());
							continue;
						} catch (InterruptedException e1) {
							System.err.println("System failed in undesirable state: " + e1.toString());
							logger.log(Level.SEVERE, "System failed in undesirable state", e1);
							System.exit(0);
						}
					} catch (InterruptedException e) {
						System.err.println("System failed in undesirable state: " + e.toString());
						logger.log(Level.SEVERE, "System failed in undesirable state", e);
						System.exit(0);
					}
					catch(Exception e) {
						logger.info("Rollback failed as Remote server is down, removing it from accessible servers list");
						servers.remove(server);
						rollbackFailed = true;
						break;
					}
					logger.info("Rollback succeeded on server: "+server.getServerID());
					rollbackFailed = false;
				}
			}
			response.setType("delete");
			response.setReturnValue(null);
			response.setMessage("Failed to delete the entry from storage");
		}
		else {
			logger.info("Initializing commit procedure");
			Message commit = new Message();
			commit.setContent(MessageContent.COMMIT);
			
			for(DatastoreInterface server: servers) {
				boolean commitFailed = true;
				while(commitFailed) {
					try {
						logger.info("Commit on server: "+server.getServerID());
						Message resp = server.send(commit);
						if (resp.content == MessageContent.NO) {
							logger.info("Commit failed on server: "+server.getServerID());
							commitFailed = true;
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting commit on server: "+server.getServerID());
							continue;
						}
					}
					catch(Abort e) {
						logger.info("Commit failed on server: "+server.getServerID());
						commitFailed = true;
						try {
							TimeUnit.SECONDS.sleep(5);
							logger.info("Reattempting commit on server: "+server.getServerID());
							continue;
						} catch (InterruptedException e1) {
							System.err.println("System failed in undesirable state: " + e1.toString());
							logger.log(Level.SEVERE, "System failed in undesirable state", e1);
							System.exit(0);
						}
					} catch (InterruptedException e) {
						System.err.println("System failed in undesirable state: " + e.toString());
						logger.log(Level.SEVERE, "System failed in undesirable state", e);
						System.exit(0);
					}
					catch(Exception e) {
						logger.info("Commit failed as Remote server is down, removing it from accessible servers list");
						servers.remove(server);
						commitFailed = true;
						break;
					}
					
					logger.info("Commit succeeded on server: "+server.getServerID());
					commitFailed = false;
				}
			}
			
			undoStorageLog = (HashMap<String,String>)storage.clone();
			ObjectOutputStream outputUndo;
			try {
				outputUndo = new ObjectOutputStream(new FileOutputStream(this.undoLog));
				outputUndo.writeObject(undoStorageLog);
				outputUndo.flush();
				outputUndo.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			storage.remove(key);
			
			redoStorageLog = (HashMap<String,String>)storage.clone();
			ObjectOutputStream outputredo;
			try {
				outputredo = new ObjectOutputStream(new FileOutputStream(this.redoLog));
				outputredo.writeObject(redoStorageLog);
				outputredo.flush();
				outputredo.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			logger.info("Successfully completed transaction on coordinator: "+this.getServerID());
			
			response.setType("put");
			response.setReturnValue(null);
			response.setMessage("Successfully deleted the entry from storage");
		}

		logger.info(response.toString());
		return response;
		
	} 


	public List<DatastoreInterface> getServers() {
		List<DatastoreInterface> servers = new ArrayList<DatastoreInterface>();	
		logger.info("Fetching other cohorts/participants");
		
		try {
			if(isMaster) {
				for(String server: this.registry.list()) {
					if(!server.equals("Server")) servers.add((DatastoreInterface)this.registry.lookup(server));
				}
			}
			else {
				Registry masterRegistry = LocateRegistry.getRegistry(this.master_address, this.master_port);
				for(String server: masterRegistry.list()) {
					if(!server.equals(this.serverID)) servers.add((DatastoreInterface)masterRegistry.lookup(server));
				}
			}
		}
		catch(Exception e) {
			System.err.println("Failed to identify other servers: " + e.toString());
			logger.log(Level.SEVERE, "Failed to identify other servers", e);
			System.exit(0);
		}

		logger.info("Successfully fetched other cohorts/participants");
		return servers;
	} 

	public synchronized Message send(Message message) throws Abort, RemoteException{
		
		logger.info("Received Message: "+message);
		
		Message response = new Message();
		Transaction transaction = message.getTransaction(); 
		
		try {
			if(message.content==MessageContent.QUERY_TO_COMMIT) {
				if(transaction.getType().equals("put")){
					preparePut(transaction.getKey(), transaction.getValue());
				}
				else if(transaction.getType().equals("delete")){
					prepareDelete(transaction.getKey());
				}
				response.setContent(MessageContent.YES);

			}
			else if (message.content==MessageContent.COMMIT) {
				storage = (HashMap<String,String>)redoStorageLog.clone();
				response.setContent(MessageContent.YES);
			}
			else if (message.content==MessageContent.ROLLBACK) {
				storage = (HashMap<String,String>)undoStorageLog.clone();
				response.setContent(MessageContent.YES);
			}
		}
		catch(Exception e) {
			message.setContent(MessageContent.NO);
			throw new Abort("transaction aborted", e);
		}
		
		logger.info("Sending Message: "+message);
		return response;
	}

	//takes in the log-file path and builds a logger object
	private static Logger getLogger(String logFile) {
		Logger logger = Logger.getLogger("server_log");  
		FileHandler fh;  

		try {  
			File log = new File(logFile);
			// if file does not exist we create a new file
			if(!log.exists()) {
				log.createNewFile();
			}
			fh = new FileHandler(logFile,true);  
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter);  

		} catch (SecurityException e) {  
			e.printStackTrace();  
		} catch (IOException e) {  
			e.printStackTrace();  
		} 

		return logger;
	}


	public static String createServerID(int port) {
		String id = null;
		try {
			InetAddress IP = InetAddress.getLocalHost();
			id = IP.getHostAddress()+"_"+String.valueOf(port);

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}

	public static void main(String args[]) 
	{ 
		try {
			int port = Integer.parseInt(args[0]);
			Registry registry = LocateRegistry.createRegistry(Integer.parseInt(args[0]));
			String currentServerID = createServerID(port);
			Server server = new Server(currentServerID, registry, port);
			//Naming.rebind("//localhost:"+port+"/Server", server);

			registry.rebind("Server", server);
			
			server.logger.info("Server started");

			InputStream input = new FileInputStream("resources/config.properties");
			Properties prop = new Properties();
            // load a properties file
            prop.load(input);
            // get discovery nodes to connect to cluster
            String[] discoveryNodes = prop.getProperty("discovery.nodes").split(",");
            
            boolean discoverySuccessful = false;
            server.logger.info("Server trying to connect to a cluster");
            
            for(String discoveryNode : discoveryNodes)
            {
            		try {
            			String[] data = discoveryNode.split(":");
            			String discoveryNodeIPAddress = data[0];
            			int discoveryNodePort = Integer.parseInt(data[1]);
            			Registry discoveredRegistry = LocateRegistry.getRegistry(discoveryNodeIPAddress, discoveryNodePort);
            			
            			for(String serverID : discoveredRegistry.list()) {
            				DatastoreInterface discoveredRegistryServer = (DatastoreInterface)discoveredRegistry.lookup(serverID);
            				if(!currentServerID.equals(discoveredRegistryServer.getServerID())) {
            					discoverySuccessful = true;
            					server.setStorage(discoveredRegistryServer.getStorage());
            					discoveredRegistryServer.registerNewServer(currentServerID, server);
            					server.logger.info("Registered current server with server: "+discoveredRegistryServer.getServerID());
            					registry.bind(discoveredRegistryServer.getServerID(), discoveredRegistryServer);
            					server.logger.info("Registered server: "+discoveredRegistryServer.getServerID()+" with current server" );
            				}
            			}
            			if(discoverySuccessful==true) break;
            		}
            		catch(Exception e){
            			continue;
            		}
            }

            if(!discoverySuccessful) {
            		server.logger.info("Could not connect to any cluster, acting as a standalone cluster");
            }
            else {
            		server.logger.info("Connected to a cluster");
            }
            
		}
		catch(ArrayIndexOutOfBoundsException e) {
			System.out.println("Please provide port as command line argument");
			System.exit(0);
		}
		catch(NumberFormatException e) {
			System.out.println("Please provide port as command line argument");
			System.exit(0);
		}
		catch(java.rmi.ConnectException e) {
			System.err.println("Could not connect to Master Server: " + e);
			System.exit(0);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("Server exception: " + e);
			System.exit(0);
		}
	}

} 

