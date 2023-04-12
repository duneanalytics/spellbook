"""Ask a question to the vector database."""
import argparse
import difflib
import os
import shutil

from chat import chat_request

parser = argparse.ArgumentParser(description="SQL translation tool")
parser.add_argument('--model_path', type=str, nargs='?', help="Path to model",
                    default="models/blur/ethereum/blur_ethereum_events.sql")
args = parser.parse_args()

model_path = args.model_path


def check_tokens(input):
    """
    Rule of thumb: 1 token ~= 4 chars in English
    https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
    """
    return int(len(input) / 4)


def chunk_string_by_token_limit(input, n=25):
    lst = input.split('\n')
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def diff_files(file1_path, file2_path, output_file_path_html, output_file_path_txt):
    """Generate a file with the differences between two files."""

    # Read the contents of both files
    with open(file1_path, 'r') as file1, open(file2_path, 'r') as file2:
        file1_lines = file1.readlines()
        file2_lines = file2.readlines()

    # Compute the differences between the two files
    differ = difflib.HtmlDiff()
    differences = differ.make_file(file1_lines, file2_lines)

    # Write the differences to the output file
    with open(output_file_path_html, 'w') as output_file:
        output_file.writelines(differences)

    # Compute the differences between the two files
    differ = difflib.unified_diff(file1_lines, file2_lines, lineterm='', fromfile=file1_path, tofile=file2_path)
    differences = ''.join(differ)

    # Write the differences to the output file
    with open(output_file_path_txt, 'w') as output_file:
        output_file.write(differences)


def get_input(model_path):
    with open(model_path, 'r') as f:
        input = f.read()
    return input


def get_instructions(rules_directory):
    instructions = []
    for file in os.listdir(rules_directory):
        if file.endswith(".txt"):
            with open(os.path.join(rules_directory, file), 'r') as f:
                instruction = f.read()
            instructions.append(instruction)
    return instructions


def copy_model(model_path):
    shutil.copyfile(model_path, model_path + ".backup")


def write_output(output, model_path):
    with open(model_path, 'w') as f:
        f.write(output)


def process_input(choice):
    try:
        code_block = choice.split("```")[1]
        if code_block.startswith('sql'):
            # Sometimes code blocks are prefaced with sql. Removing if so.
            code_block = code_block[len('sql'):]
        return code_block
    except IndexError:
        print("Error processing input: " + choice)
        return choice


# This endpoint is in beta and is currently free
# https://openai.com/blog/gpt-3-edit-insert
model = "gpt-4"
copy_model(model_path)
input = get_input(model_path)

rules_directory = "rules/spark"
instructions = get_instructions(rules_directory)
inputs = chunk_string_by_token_limit(input)


output = []
for i, input_piece in enumerate(inputs):
    print(f'Iterating: {round(100 * (i / len(inputs)), 3)}%')
    input = "\n".join(input_piece)
    response = chat_request(model, instructions, input)
    choice = response["choices"][0]['message']['content']
    processed_choice = process_input(choice)
    output.append(processed_choice)

write_output("".join(output), model_path + ".new")
print(f'Diffing {model_path} and {model_path + ".new"}')
diff_files(model_path, model_path + ".new", model_path + ".diff.html", model_path + ".diff.txt")
