1.How to Compile and Execute the code:
You're going to need Python installed, preferably Python 3

2.How to run the data creation:
You need to open the terminal and open the project folder,
now use this command to generate the dataToIndex.txt file:
    python3 DataCreation.py -k keyFile.txt -n 50 -d 4 -m 5 -l 4

You can customize this command to generate a file to your needs:
-n number of rows of the generated file
-d maximum nesting level
-m maximum number of keys per value
-l maximum length of the string values
-k is the name of the file that contains the key names and their data types (you can add more if you want)

3.How to run the servers:
You need to open the terminal and open the project folder, and execute this command:
    python3 KVServer.py -a 127.0.0.1 -p 8001

You can customize it according to your needs:
-a the IP address to bind to
-p the port number to listen to

In order to run multiple server you need to open multiple instances of the terminal and run the same command and change the port (e.g 8002/8003/...)


4.How to run the broker
Again inside the project folder you need to run this command:
    python3 KVBroker.py -s serverFile.txt -i dataToIndex.txt -k 3

You can customize the command to your needs:
-s is the name of the file that contains the KV Server IPs and ports locally (you can customize it if you want)
-i the name of the file that the broker is going to use to get the data in order to share it with the servers
-k is the replication factor, you can set it whatever you want

5.Now you should get the "Enter command:" prompt, so you can start sending commands to the servers.
Available commands:
a)GET - GET person1 : should return the value of the record with high level key "person1"
b)DELETE - DELETE person1 : should delete the record of person1 from all the servers that is stored to
c)QUERY - QUERY person1.name.age : should return the age value that is nested inside the name key of the person1.