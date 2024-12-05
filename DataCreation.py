import random
import string


def random_key(type, keys,Ints,Strings,Floats):
    if type == 'string':
        return random.choice(Strings)
    elif type == 'int':
        return random.choice(Ints)
    else:
        return random.choice(Floats)

def generate_value(nesting_level,keys,Ints,Strings,Floats):
    value_type = random.choice(["int", "float", "string"])
    key = random_key(value_type, keys, Ints, Strings, Floats)
    if nesting_level == 0:
        if value_type == "int":
            return {f"{key}":random.randint(0, 100)}
        elif value_type == "float":
            return {f"{key}":round(random.uniform(0, 100), 2)}
        else:
            return {f"{key}":''.join(random.choice(string.ascii_letters) for _ in range(random.randint(1, max_length)))}
    else:
        num_keys = random.randint(0, max_keys)
        return {
            f"{key}": generate_value(nesting_level - 1,keys,Ints,Strings,Floats)
            for i in range(num_keys)
        }

def create_data(key_file, num_lines, max_nesting, max_keys, max_length):
    with open(key_file, 'r') as f:
        keys = [line.strip().split() for line in f]
    listString =[]
    listInt = []
    listFloat = []
    for k in keys:
        if k[1]== 'string':
            listString.append(k[0])
        elif k[1]== 'int':
            listInt.append(k[0])
        else:
            listFloat.append(k[0])
    data = []
    for i in range(num_lines):
        key = f"person{i}"  # You can use random keys here if needed
        value = generate_value(random.randint(0, max_nesting),keys,listInt,listString,listFloat)
        data.append(f'"{key}" : {value}')

    return data

if __name__ == "__main__":
    key_file = "keyFile.txt"
    num_lines = 3
    max_nesting = 1
    max_keys = 2
    max_length = 3

    generated_data = create_data(key_file, num_lines, max_nesting, max_keys, max_length)

    with open("dataToIndex.txt", "w") as file:
        for line in generated_data:
            file.write(line + '\n')