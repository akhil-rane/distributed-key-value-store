package com.cs6650;

public class Accepted {
	
	private String serverID;
	
	private long proposalNumber;
	
	private Transaction value;

	public long getProposalNumber() {
		return proposalNumber;
	}

	public void setProposalNumber(long proposalNumber) {
		this.proposalNumber = proposalNumber;
	}

	public Transaction getValue() {
		return value;
	}

	public void setValue(Transaction value) {
		this.value = value;
	}

	public String getServerID() {
		return serverID;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}
}