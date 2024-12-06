import random
import string
import argparse
import json


def parse_keyfile(keyfile_path):
    #Parse the keyFile.txt to extract keys and their data types
    key_types = {}
    with open(keyfile_path, 'r') as f:
        for line in f:
            key, data_type = line.strip().split()
            key_types[key] = data_type
    return key_types


def generate_random_string(max_length):
    #Generate a random alphanumeric string of length up to max_length
    length = random.randint(1, max_length)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_value(data_type, max_length):
    #Generate a value based on its data type
    if data_type == "string":
        return generate_random_string(max_length)
    elif data_type == "int":
        return random.randint(1, 100)  # Random integer between 1 and 100
    elif data_type == "float":
        return round(random.uniform(1, 100), 2)  # Random float between 1.00 and 100.00
    return None


def generate_data(keys, max_keys, max_length, max_depth, current_depth=0):
    #recursively generate a nested dictionary of key-value pairs
    if current_depth >= max_depth:
        return {}  # No further nesting at max depth

    num_keys = random.randint(1, max_keys)
    data = {}
    for _ in range(num_keys):
        key = random.choice(list(keys.keys()))
        data_type = keys[key]

        # generate a value: Nested dictionary or terminal value
        if current_depth < max_depth - 1 and random.choice([True, False]):
            data[key] = generate_data(keys, max_keys, max_length, max_depth, current_depth + 1)
        else:
            data[key] = generate_value(data_type, max_length)

    return data

def json_to_string_file(input_file, output_file):
    #Transform JSON data to a string format with each high-level key on one line
    with open(input_file, 'r') as infile:
        data = json.load(infile)

    with open(output_file, 'w') as outfile:
        for key, value in data.items():
            # Convert the key-value pair to a JSON string and write it to a line
            line = json.dumps({key: value})
            line = line.replace(",",";")
            line = line[1:-1]
            outfile.write(line + "\n")

def create_data(keyfile_path, num_lines, max_depth, max_keys, max_length):
    """Create a dataset of randomly generated key-value pairs."""
    keys = parse_keyfile(keyfile_path)
    dataset = {}

    for i in range(1, num_lines + 1):
        key = f"person{i}"
        dataset[key] = generate_data(keys, max_keys, max_length, max_depth)

    return dataset


if __name__ == "__main__":
    # Argument parser for command-line options
    parser = argparse.ArgumentParser(description="Generate a dataset for a KV store.")
    parser.add_argument("-k", "--keyfile", required=True, help="Path to key file.")
    parser.add_argument("-n", "--num_lines", type=int, required=True, help="Number of lines to generate.")
    parser.add_argument("-d", "--max_depth", type=int, required=True, help="Maximum nesting depth.")
    parser.add_argument("-m", "--max_keys", type=int, required=True, help="Maximum keys per nesting level.")
    parser.add_argument("-l", "--max_length", type=int, required=True, help="Maximum string length.")

    args = parser.parse_args()

    # Generate the data
    dataset = create_data(
        keyfile_path=args.keyfile,
        num_lines=args.num_lines,
        max_depth=args.max_depth,
        max_keys=args.max_keys,
        max_length=args.max_length
    )

    # Output to JSON
    with open("output.json", "w") as outfile:
        json.dump(dataset, outfile, indent=4)
    #transform JSON to txt
    with open("dataToIndex.txt", "w") as outfile:
        json_to_string_file("output.json", "dataToIndex.txt")
    print("Dataset successfully generated and saved to dataToIndex.txt")
