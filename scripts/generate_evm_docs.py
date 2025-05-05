import os
from jinja2 import Environment, FileSystemLoader

# --- Configuration ---
# Assumes the script is run from the workspace root
TEMPLATE_DIR = "sources/_base_sources/evm"
TEMPLATE_NAME = "_template.md.j2" # Use the markdown template
OUTPUT_DIR = "sources/_base_sources/evm"
OUTPUT_FILENAME_SUFFIX = "_docs_block.md" # Output markdown files
# ---------------------

# --- Ethereum specific withdrawal doc block ---
# (Copied from the template, kept separate for conditional inclusion)
ETH_WITHDRAWALS_DOC = """
{% docs ethereum_withdrawals_doc %}

The `ethereum.withdrawals` table contains information about withdrawals on the ethereum blockchain. It includes:

- Block number and timestamp
- Transaction hash
- From address
- To address
- Amount
- Index

This table is used for analyzing withdrawals from the becaon chain on the ethereum network.

{% enddocs %}
"""

def main():
    print("Starting EVM documentation file generation...")

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

    # --- Setup Jinja ---    
    try:
        template_loader = FileSystemLoader(searchpath=TEMPLATE_DIR)
        env = Environment(loader=template_loader, trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(TEMPLATE_NAME)
        print(f"Loaded template: {os.path.join(TEMPLATE_DIR, TEMPLATE_NAME)}")
    except Exception as e:
        print(f"Error loading Jinja template {TEMPLATE_NAME} from {TEMPLATE_DIR}: {e}")
        return

    # --- Generate Files ---    
    generated_count = 0
    errors_count = 0
    for chain in chains:
        try:
            print(f"  Generating documentation file for: {chain}...")
            output_content = template.render(chain_name=chain)
            
            # Conditionally add the Ethereum withdrawal block
            if chain == 'ethereum':
                output_content += "\n\n" + ETH_WITHDRAWALS_DOC # Add spacing before appending
                print(f"    -> Appending Ethereum specific withdrawal docs.")

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