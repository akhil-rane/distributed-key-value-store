# distributed-key-value-store
A basic implementation of a distributed key-value store that achieves fault tolerance and consensus of updates amongst replica servers using Paxos

# Steps to compile the Server and Client codes: 
Go to project folder and enter following commands
- mkdir bin
- mkdir logs
- javac -d bin src/com/cs6650/*java

# Commands to run the Server and Client: 
Go to project folder and enter following commands 
- Server: java -cp bin com.cs6650.Server <server_port>
- Client: java -cp bin com.cs6650.Client <server_ip_address> <server_port> 
