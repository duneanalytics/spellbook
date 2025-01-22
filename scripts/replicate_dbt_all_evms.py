import os
import sys

# List of valid evm chains
chains = [
    "arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum",
    "fantom", "gnosis", "linea", "mantle", "optimism", "polygon", "scroll",
    "sei", "zkevm", "zksync", "zora"
]


# Function to replace chain name in a string
def replace_chain_name(content, old_chain, new_chain):
    return content.replace(old_chain, new_chain)


# Function to replicate the directory structure
def replicate_directory_structure(template_dir, chains):
    # Infer the template chain name from the directory name
    template_chain = os.path.basename(template_dir)

    # Check if the template chain is in the list of valid chains
    if template_chain not in chains:
        print(f"Error: The inferred template chain '{template_chain}' is not in the list of valid chains.")
        sys.exit(1)

    # Infer the output base directory
    output_base_dir = os.path.dirname(template_dir)

    for chain in chains:
        # Skip the template chain itself
        if chain == template_chain:
            continue

        # Create a new directory for each chain
        new_dir = os.path.join(output_base_dir, chain)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)

        # Copy and adapt files from the template directory
        for root, dirs, files in os.walk(template_dir):
            for file in files:
                # Construct the source file path
                src_file_path = os.path.join(root, file)

                # Read the file content
                with open(src_file_path, 'r') as f:
                    content = f.read()

                # Replace any reference of the template chain name within the file content
                new_content = replace_chain_name(content, template_chain, chain)

                # Replace the template chain name in the file name
                new_file_name = file.replace(template_chain, chain)
                new_file_path = os.path.join(new_dir, new_file_name)

                # Write the new content to the new file
                with open(new_file_path, 'w') as f:
                    f.write(new_content)

                print(f"Created file: {new_file_path}")


# Main function to handle command line arguments
def main():
    if len(sys.argv) != 2:
        print("Usage: python replicate_dbt_all_evms.py <template_directory_path>")
        sys.exit(1)

    # Get the template directory from command line arguments
    template_dir = sys.argv[1]

    # Check if the provided directory exists
    if not os.path.exists(template_dir):
        print(f"Error: The directory '{template_dir}' does not exist.")
        sys.exit(1)

    # Call the function to replicate the directory structure
    replicate_directory_structure(template_dir, chains)


if __name__ == "__main__":
    main()
