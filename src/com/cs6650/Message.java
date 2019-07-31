package com.cs6650;

import java.io.Serializable;

public class Message implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4756106675842475317L;
	
	enum MessageContent
	{
		QUERY_TO_COMMIT,
		YES,
		COMMIT, 
		ROLLBACK, 
		NO
	}
	
	MessageContent content;
	Transaction transaction;
	
	public MessageContent getContent() {
		return content;
	}
	public void setContent(MessageContent content) {
		this.content = content;
	}
	public Transaction getTransaction() {
		return transaction;
	}
	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}
	@Override
	public String toString() {
		return "Message [content=" + content + ", transaction=" + transaction
				+ "]";
	}
	
	
}
