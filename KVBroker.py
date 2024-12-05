import socket
import random

# Configuration and Setup
servers = []  # List to store server IPs and ports
replication_factor = 2  # Default replication factor

# Function to read server IPs and ports from serverFile.txt
def read_server_file(filename):
    """Reads server IP addresses and ports from a file.

    Args:
        filename (str): The name of the file containing server information.

    Returns:
        list: A list of tuples, where each tuple is (server_ip, server_port).
    """
    global servers
    try:
        with open(filename, 'r') as f:
            for line in f:
                server_info = line.strip().split()
                if len(server_info) == 2:
                    server_ip, server_port = server_info, int(server_info[1])
                    servers.append((server_ip, server_port))
    except Exception as e:
        print(f"ERROR reading server file: {e}")
        exit(1)

# Function to read data to index from dataToIndex.txt
def read_data_to_index(filename):
    """Reads data to be indexed from a file.

    Args:
        filename (str): The name of the file containing data to index.

    Returns:
        list: A list of strings, where each string is a line of data to index.
    """
    data = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                data.append(line.strip())
    except Exception as e:
        print(f"ERROR reading data to index file: {e}")
        exit(1)
    return data

# Function to send a command to a server and receive a response
def send_command(server_ip, server_port, command):
    """Sends a command to a server and receives the response.

    Args:
        server_ip (str): The IP address of the server.
        server_port (int): The port number the server is listening on.
        command (str): The command to be sent.

    Returns:
        str: The server's response, or an error message if there was a problem.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((server_ip, int(server_port)))
            sock.sendall(command.encode())
            response = sock.recv(1024).decode()  # Adjust buffer size as needed

            return response
    except Exception as e:
        return f"ERROR: {e}"

# Function to handle GET command
def handle_get(key):
    """Handles the GET command, querying all servers for the given key.

    Args:
        key (str): The key to be retrieved.
    """
    available_servers = 0
    responses = []
    for server_ip, server_port in servers:
        response = send_command(server_ip[0], server_ip[1], f"GET {key}")
        responses.append(response)
        if "NOT FOUND" not in response and "ERROR" not in response:
            available_servers += 1
    print(f"Available servers: {available_servers}")
    if available_servers < replication_factor:
        print("WARNING: Fewer than k servers available. Results may be inconsistent.")

    for response in responses:
        print(response)

# Function to handle DELETE command
def handle_delete(key):
    """Handles the DELETE command, deleting the key from all servers.

    Args:
        key (str): The key to be deleted.
    """
    for server_ip, server_port in servers:
        response = send_command(server_ip, server_port, f"DELETE {key}")
        if "ERROR" in response:
            print(f"ERROR: Could not delete from server {server_ip}:{server_port}. Deletion aborted.")
            return
    print("Key deleted successfully from all servers.")

# Function to handle QUERY command
def handle_query(keypath):
    """Handles the QUERY command, querying all servers for the value at the given keypath.

    Args:
        keypath (str): The keypath to query (e.g., 'person2.address.street').
    """
    available_servers = 0
    responses = []
    for server_ip, server_port in servers:
        response = send_command(server_ip, server_port, f"QUERY {keypath}")
        responses.append(response)
        if "NOT FOUND" not in response and "ERROR" not in response:
            available_servers += 1
    print(f"Available servers: {available_servers}")

    if available_servers < replication_factor:
        print("WARNING: Fewer than k servers available. Results may be inconsistent.")

    for response in responses:
        print(response)

# Function to index data on the servers
def index_data(data):
    """Indexes data on the specified servers.

    Args:
        data (list): A list of strings, where each string represents a data entry to index.
    """
    global servers, replication_factor
    for data_entry in data:
        # Randomly select k servers for replication
        selected_servers = random.sample(servers, replication_factor)
        for server_ip, server_port in selected_servers:
            command = f"PUT {data_entry}"
            response = send_command(server_ip[0], server_ip[1], command)
            print(response)
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