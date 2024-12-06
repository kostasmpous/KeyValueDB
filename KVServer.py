import json
import socket
import threading
import re

class TrieNode:
    def __init__(self):
        # Each node has a dictionary of children and a flag to mark the end of a word
        self.children = {}
        self.is_end_of_word = False
        self.value = None


class Trie:
    def __init__(self):
        # The root of the trie doesn't store any character
        self.root = TrieNode()

    #insert a word in the trie
    def insert(self, word,value):
        node = self.root
        for char in word:
            if char not in node.children:
                # Add a new node for the character if it doesn't exist
                node.children[char] = TrieNode()
            node = node.children[char]
        node.value = value#add the whole string of data to the last node
        node.is_end_of_word = True# Mark the end of the word

    #search the trie for a word and return the data
    def search(self, word):
        node = self.root#start the search from the root
        for char in word:#for each character in the word
            if char not in node.children:#if the character is not in the children node return false
                return False
            node = node.children[char]#else traverse further
        return node.value

    # delete a word from the trie if exists
    def delete(self, word):
        def _delete(node, word, depth):
            if not node:#Word is not found
                return False
            if depth == len(word):#If we've reached the end of the word
                if node.is_end_of_word:
                    node.is_end_of_word = False#Unmark the end of word
                    return len(node.children) == 0# If the current node has no children, it can be deleted
                return False
            #Process the next character
            char = word[depth]
            if char in node.children:
                should_delete_child = _delete(node.children[char], word, depth + 1)

                # If the child node can be deleted, remove it
                if should_delete_child:
                    del node.children[char]
                    # Return True if the current node has no other children and is not the end of another word
                    return len(node.children) == 0 and not node.is_end_of_word
            return False

        _delete(self.root, word, 0)


# Initialize the Trie
trie = Trie()

# Function to handle incoming client connections
def handle_client(client_socket):
    while True:
        try:
            # Receive data from the client
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                break  # Connection closed
            # Handle different commands
            if 'PUT' in data:
                pattern = r'\bperson\d+\b'
                key = re.search(pattern,data)#in data find the person high level key
                data = data.replace("PUT ","")# remove the PUT
                trie.insert(str(key.group()),data)#insert the data
                client_socket.send('OK'.encode('utf-8'))
            elif 'GET' in data:
                pattern = r'\bperson\d+\b'
                key = re.search(pattern,data)
                value = trie.search(key.group())#search the data after formating it
                if value is not False:
                    client_socket.send(value.encode('utf-8'))
                else:
                    client_socket.send('NOTFOUND'.encode('utf-8'))
            elif 'DELETE' in data:
                pattern = r'\bperson\d+\b'
                key = re.search(pattern, data)
                if trie.delete(key.group())!=False:#try delete the data
                    client_socket.send('OK'.encode('utf-8'))
                else:
                    client_socket.send('NOTFOUND'.encode('utf-8'))#if not in the trie
            elif 'QUERY' in data:
                pattern = r'\bperson\d+\b'
                key = re.search(pattern, data)
                value = trie.search(key.group())#search for the data after formatting it
                if value is not False:
                    value = value.replace(";",",")
                    value = "{"+value+"}"
                    jsn = json.loads(value)#formating the string to load it as json and traverse it
                    data= data.replace("QUERY ","")
                    keys = data.split(".")
                    for k in keys:
                        if isinstance(jsn,dict) and k in jsn:#traverse json object
                            jsn = jsn[k]
                        else:
                            client_socket.send('NOTFOUND'.encode('utf-8'))
                    result = data +" : "+ str(jsn)#make the result and return it
                    client_socket.send(result.encode('utf-8'))
                else:
                    client_socket.send('NOTFOUND'.encode('utf-8'))
        except Exception as e:
            client_socket.send(f'ERROR: {str(e)}'.encode('utf-8'))

# Main server function
def start_server(ip_address, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip_address, port))
    server_socket.listen(1)
    print(f'Server listening on {ip_address}:{port}')

    while True:
        client_socket, client_address = server_socket.accept()
        print(f'Accepted connection from {client_address}')

        # Handle client connection in a separate thread
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 5:
        print('Usage: python kvServer.py -a <ip_address> -p <port>')
        sys.exit(1)

    ip_address = sys.argv[2]
    port = int(float(sys.argv[4]))

    start_server(ip_address, port)