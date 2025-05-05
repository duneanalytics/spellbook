import os

# --- Configuration ---
# Assumes the script is run from the workspace root
TEMPLATE_DIR = "sources/_base_sources/evm"
TEMPLATE_NAME = "_template.md.j2" # Use the markdown template
OUTPUT_DIR = "sources/_base_sources/evm"
OUTPUT_FILENAME_SUFFIX = "_docs_block.md" # Output markdown files
# ---------------------

def main():
    print("Starting EVM documentation file generation (using string replacement)...")

    # Ensure output directory exists
    if not os.path.exists(OUTPUT_DIR):
        print(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR)

    # --- Use Hardcoded Chain List --- (Same as YAML generation script)
    chains = [
        "abstract", "apechain", "arbitrum", "nova", "avalanche_c", "b3", "base",
        "berachain", "blast", "bnb", "bob", "boba", "celo", "corn", "degen",
        "ethereum", "fantom", "flare", "gnosis", "goerli", "ink", "kaia", "lens", "linea",
        "mantle", "mode", "opbnb", "optimism", "polygon", "zkevm", "ronin",
        "scroll", "sei", "shape", "sonic", "sophon", "unichain", "viction",
        "worldchain", "zksync", "zora"
    ]
    print(f"Using hardcoded list of {len(chains)} chains.")

    # --- Read Template Content ---    
    template_path = os.path.join(TEMPLATE_DIR, TEMPLATE_NAME)
    try:
        with open(template_path, 'r') as f:
            template_content = f.read()
        print(f"Loaded template content from: {template_path}")
    except Exception as e:
        print(f"Error reading template file {template_path}: {e}")
        return

    # --- Generate Files using String Replacement ---    
    generated_count = 0
    errors_count = 0
    for chain in chains:
        try:
            print(f"  Generating documentation file for: {chain}...")
            # Use simple string replacement
            output_content = template_content.replace("{{ chain_name }}", chain)
            
            output_filename = f"{chain}{OUTPUT_FILENAME_SUFFIX}"
            output_path = os.path.join(OUTPUT_DIR, output_filename)
            
            with open(output_path, 'w') as f:
                f.write(output_content)
            # print(f"    Successfully wrote: {output_path}")
            generated_count += 1
        except Exception as e:
            print(f"    Error generating file for {chain}: {e}")
            errors_count += 1

    print("\nDocumentation generation complete.")
    print(f"  Successfully generated: {generated_count} files")
    if errors_count:
        print(f"  Errors encountered:     {errors_count} files")

if __name__ == "__main__":
    main() 