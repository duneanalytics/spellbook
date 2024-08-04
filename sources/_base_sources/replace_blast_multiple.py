import os
import re

def replace_in_file(content, old_word, new_word):
    # Use regex for case-insensitive replacement
    new_content = re.sub(re.escape(old_word), new_word, content, flags=re.IGNORECASE)
    return new_content

def create_new_file(new_file_path, content):
    with open(new_file_path, 'w') as file:
        file.write(content)

def process_files_for_words(source_path, dest_path, old_word, words_list, files):
    for word in words_list:
        for file in files:
            source_file = os.path.join(source_path, file)
            new_file_name = re.sub(re.escape(old_word), word, file, flags=re.IGNORECASE)
            new_file_path = os.path.join(dest_path, new_file_name)
            
            with open(source_file, 'r') as f:
                content = f.read()
            
            # Replace text within the file
            new_content = replace_in_file(content, old_word, word)
            
            # Create the new file with the new content
            create_new_file(new_file_path, new_content)
            
            print(f'Processed file for {word}: {new_file_path}')

# Define the source and destination paths
source_path = '/Users/FloRyan/Documents/GitHub/spellbook/sources/_base_sources'
dest_path = '/Users/FloRyan/Documents/GitHub/spellbook/sources/_base_sources'

# Define the old word
old_word = 'blast'

# Define the list of new words
words_list = [
    'arbitrum', 'avalanche_c', 'base', 'bnb', 'bob', 'celo', 'degen', 'ethereum',
    'fantom', 'gnosis', 'linea', 'mantle', 'mode', 'optimism', 'polygon', 'scroll',
    'sei', 'zkevm', 'zksync', 'zora'
]

# Define the files to process
files = [
    'blast_docs_block.md',
    'blast_base_sources.yml'
]

# Process the files for each word in the list
process_files_for_words(source_path, dest_path, old_word, words_list, files)
