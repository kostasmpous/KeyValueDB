import socket
import random
import re

# Configuration and Setup
servers = []  # List to store server IPs and ports
replication_factor = 2  # Default replication factor
map_data = {}#map the data to the servers

# Function to read server IPs and ports from serverFile.txt
def read_server_file(filename):
    global servers
    try:
        with open(filename, 'r') as f:
            for line in f:
                server_info = line.strip().split()
                if len(server_info) == 2:
                    server_ip, server_port = server_info, int(server_info[1])
                    servers.append((server_ip, server_port))#append servers info to global list
    except Exception as e:
        print(f"ERROR reading server file: {e}")
        exit(1)

# Function to read data to send it to servers from dataToIndex.txt
def read_data_to_index(filename):
    data = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                data.append(line.strip())#read data to send to servers
    except Exception as e:
        print(f"ERROR reading data to index file: {e}")
        exit(1)
    return data

# Function to send a command to a server and receive a response
def send_command(server_ip, server_port, command):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((server_ip, int(server_port)))
            sock.sendall(command.encode())
            response = sock.recv(1024).decode()  # Adjust buffer size as needed

            return response
    except Exception as e:
        return f"ERROR: {e}"

# Function to handle the restore of lost data (in case a server was down for a while)
def check_backup(server_ip, key):
    flag = False
    if key in map_data:#check  if key exists
        for value1,value2 in map_data.get(key):
            if value1 == server_ip:#checking if this server should have this data but lost it
                flag = True
        if flag:
            response=""
            for value1,value2 in map_data.get(key):#find which server contains a backup
                if value1 != server_ip:
                    response = send_command(value1[0], value1[1], f"GET {key}")#get the backup
            send_command(server_ip[0], server_ip[1], f"PUT {response}")#restore  it to the server

#Handles the GET command, querying all servers for the given key.
def handle_get(key):
    available_servers = 0#track how may servers are up
    responses = []
    for server_ip, server_port in servers:
        response = send_command(server_ip[0], server_ip[1], f"GET {key}")
        responses.append(response)
        if "ERROR" not in response:
            available_servers += 1
        if "NoneType" in response:
            available_servers += 1
            check_backup(server_ip,key)
    print(f"Available servers: {available_servers}")
    if available_servers < replication_factor:#if needed warn the user about the available server number
        print("WARNING: Fewer than k servers available. Results may be inconsistent.")
    flag = "NOT FOUND"
    for response in responses:
        if response != "NOTFOUND" and "ERROR" not in response:
            flag = response
    print(flag)


# Function to handle DELETE command
def handle_delete(key):
    for server_ip, server_port in servers:
        response = send_command(server_ip[0], server_ip[1], f"DELETE {key}")
        if "ERROR" in response:
            print(f"ERROR: Could not delete from server {server_ip}:{server_port}. Deletion aborted.")
            return
    if map_data.get(key):
        map_data.pop(key)#remove the data from map
    print("Key deleted successfully from all servers.")

# Function to handle QUERY command
def handle_query(keypath):
    available_servers = 0#track available servers
    responses = []
    for server_ip, server_port in servers:
        response = send_command(server_ip[0], server_ip[1], f"QUERY {keypath}")
        responses.append(response)
        if "NOT FOUND" not in response and "ERROR" not in response:
            available_servers += 1
    print(f"Available servers: {available_servers}")
    if available_servers < replication_factor:#warn user about available servers
        print("WARNING: Fewer than k servers available. Results may be inconsistent.")
    flag = "NOT FOUND"
    for response in responses:
        if response != "NOTFOUND":
            flag = response
    print(flag)

# Function to index data on the servers
def index_data(data):
    global servers, replication_factor
    for data_entry in data:
        # Randomly select k servers for replication
        pattern = r'\bperson\d+\b'
        person = re.search(pattern, data_entry)#get the person high level key to add it in the backup map
        selected_servers = random.sample(servers, replication_factor)
        map_data[person.group()] = selected_servers#add the value to the backup map
        for server_ip, server_port in selected_servers:
            command = f"PUT {data_entry}"
            response = send_command(server_ip[0], server_ip[1], command)
            if "ERROR" in response:
                print(f"ERROR indexing data on server {server_ip}:{server_port}: {response}")

# Main Broker Loop
if __name__ == "__main__":
    import argparse  # Import the argparse module

    # Create an argument parser
    parser = argparse.ArgumentParser(description="KV Broker")

    # Add arguments for server file, data file, and replication factor
    parser.add_argument('-s', '--server_file', type=str, required=True, help='File containing server IPs and ports')
    parser.add_argument('-i', '--data_file', type=str, required=True, help='File containing data to index')
    parser.add_argument('-k', '--replication_factor', type=int, default=2, help='Replication factor (k)')

    # Parse the command-line arguments
    args = parser.parse_args()

    # Read server information from the provided file
    read_server_file(args.server_file)

    # Read data to be indexed from the provided file
    data_to_index = read_data_to_index(args.data_file)

    # Set replication factor from command-line argument
    replication_factor = args.replication_factor

    # Index the data on the servers
    index_data(data_to_index)

    while True:
        command = input("Enter command: ")
        parts = command.split()
        command_type = parts[0].upper()

        if command_type == "GET" and len(parts) == 2:
            handle_get(parts[1])
        elif command_type == "DELETE" and len(parts) == 2:
            handle_delete(parts[1])
        elif command_type == "QUERY" and len(parts) == 2:
            handle_query(parts[1])
        else:
            print("Invalid command.")