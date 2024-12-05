import socket
import threading

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

    def insert(self, word,value):
        """Insert a word into the trie."""
        node = self.root
        for char in word:
            if char not in node.children:
                # Add a new node for the character if it doesn't exist
                node.children[char] = TrieNode()
            node = node.children[char]
        # Mark the end of the word
        node.value = value
        node.is_end_of_word = True

    def search(self, word):
        """Search for a word in the trie. Returns True if found, otherwise False."""
        node = self.root
        for char in word:
            if char not in node.children:
                return False
            node = node.children[char]
        return word + node.value

    def starts_with(self, prefix):
        """Check if there's any word in the trie that starts with the given prefix."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return False
            node = node.children[char]
        return True

    def delete(self, word):
        """Delete a word from the trie if it exists."""
        def _delete(node, word, depth):
            if not node:
                return False  # Word not found

            # Base case: If we've reached the end of the word
            if depth == len(word):
                if node.is_end_of_word:
                    node.is_end_of_word = False  # Unmark the end of word
                    # If the current node has no children, it can be deleted
                    return len(node.children) == 0
                return False

            # Recursive case: Process the next character
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

            # Split the data into command and arguments
            command, *args = data.split()
            # Handle different commands
            if command == 'PUT':
                key, value = args, ' '.join(args[1:])
                print(value)
                trie.insert(str(key[0]).strip('"'),value)
                client_socket.send('OK'.encode('utf-8'))
            elif command == 'GET':
                print(trie)
                key = args
                print(key[0])
                print(type(key[0]))
                value = trie.search(key[0])
                if value is not False:
                    client_socket.send(value.encode('utf-8'))
                else:
                    client_socket.send('NOTFOUND'.encode('utf-8'))
            elif command == 'DELETE':
                key = args
                if trie.delete(key):
                    client_socket.send('OK'.encode('utf-8'))
                else:
                    client_socket.send('NOTFOUND'.encode('utf-8'))
            elif command == 'QUERY':
                key_path = args
                keys = key_path.split('.')
                value = trie.search(keys)
                if value is not None:
                    for key in keys[1:]:
                        if isinstance(value, dict) and key in value:
                            value = value[key]
                        else:
                            value = None
                            break
                    if value is not None:
                        client_socket.send(str(value).encode('utf-8'))
                    else:
                        client_socket.send('NOTFOUND'.encode('utf-8'))
                else:
                    client_socket.send('NOTFOUND'.encode('utf-8'))
            else:
                client_socket.send('ERROR: Invalid command'.encode('utf-8'))
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