package com.cs6650;

import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.List;

// RMI Interface
public interface DatastoreInterface extends Remote{
	public Response put(String key, String value) throws RemoteException;
	public Response get(String key) throws RemoteException;
	public Response delete(String key) throws RemoteException;
	public HashMap<String, String> getStorage() throws RemoteException;
	public String getServerID() throws RemoteException;
	public Promise prepare(long proposalNumber) throws RemoteException;
	public Accepted accept(long proposalNumber, Transaction value) throws RemoteException;
	public void invokeLearner(Accepted accepted) throws RemoteException;
	public void registerNewServer(String currentServerID, DatastoreInterface server) throws RemoteException, AlreadyBoundException;
}

