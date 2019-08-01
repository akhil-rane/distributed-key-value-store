# distributed-key-value-store
A basic implementation of a distributed key-value store that achieves fault tolerance and consensus of updates amongst replica servers using Paxos

# clone the repository using following command:
- git clone https://github.com/akhil-rane/distributed-key-value-store.git

# steps to compile the server and client codes: 
Go to project folder and enter following commands
- mkdir bin
- mkdir logs
- javac -d bin src/com/cs6650/*java

# commands to run the server and client: 
Go to project folder and enter following commands 
- Server: java -cp bin com.cs6650.Server <server_port>
- Client: java -cp bin com.cs6650.Client <server_ip_address> <server_port> 
