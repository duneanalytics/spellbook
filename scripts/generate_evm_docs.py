import os
import re # Import regex module
from jinja2 import Environment, FileSystemLoader # Re-add Jinja import

# --- Configuration ---
# Assumes the script is run from the workspace root
TEMPLATE_DIR = "sources/_base_sources/evm"
TEMPLATE_NAME = "_template.md.j2" # Use the markdown template
OUTPUT_DIR = "sources/_base_sources/evm"
OUTPUT_FILENAME_SUFFIX = "_docs_block.md" # Output markdown files
# ---------------------

# --- Remove the separate ETH_WITHDRAWALS_DOC constant ---
# ETH_WITHDRAWALS_DOC = """ ... """

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
        "mantle", "mode", "opbnb", "optimism", "plume", "polygon", "zkevm", "ronin",
        "scroll", "sei", "shape", "sepolia", "sonic", "sophon", "unichain", "viction", # Added sepolia back
        "worldchain", "zksync", "zora"
    ]
    print(f"Using hardcoded list of {len(chains)} chains.")

    # --- Setup Jinja ---    
    try:
        template_loader = FileSystemLoader(searchpath=TEMPLATE_DIR)
        env = Environment(loader=template_loader, trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(TEMPLATE_NAME)
        print(f"Loaded template: {os.path.join(TEMPLATE_DIR, TEMPLATE_NAME)}")
        # Remove the regex loading logic
        # with open(template_path, 'r') as f:
        #     template_content_full = f.read()
        #     template_content_base = re.sub(r"{% docs ethereum_withdrawals_doc %}.*?{% enddocs %}", "", template_content_full, flags=re.DOTALL | re.MULTILINE)
        #     template_content_base = template_content_base.strip()
    except Exception as e:
        print(f"Error loading Jinja template {TEMPLATE_NAME} from {TEMPLATE_DIR}: {e}") # Corrected error message source
        return

    # --- Generate Files ---    
    generated_count = 0
    errors_count = 0
    for chain in chains:
        try:
            print(f"  Generating documentation file for: {chain}...")
            # Use template.render() directly
            output_content = template.render(chain_name=chain)
            
            # --- Remove conditional append logic ---
            # if chain == 'ethereum':
            #     output_content += "\n\n" + ETH_WITHDRAWALS_DOC
            #     print(f"    -> Appending Ethereum specific withdrawal docs.")

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