import os
import subprocess
import shutil

# need to make sure dbt deps are installed for all subprojects

# Define the base directory of the repo, adjusting for script's location
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dbt_subprojects'))

# Define the list of subprojects to compile
subprojects = ['daily_spellbook', 'dex', 'hourly_spellbook', 'nft', 'solana', 'tokens']

# Directory to store the collected manifest.json files
output_dir = os.path.join(base_dir, 'manifests')
os.makedirs(output_dir, exist_ok=True)

def run_dbt_compile(project_dir):
    """Run `dbt compile` in the specified project directory."""
    try:
        result = subprocess.run(['dbt', 'compile'], cwd=project_dir, check=True, capture_output=True, text=True)
        print(f"Successfully compiled {project_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error compiling {project_dir}: {e.stdout}\n{e.stderr}")

def collect_manifest(project_dir):
    """Collect the manifest.json file from the project directory."""
    manifest_path = os.path.join(project_dir, 'target', 'manifest.json')
    if os.path.exists(manifest_path):
        dest_path = os.path.join(output_dir, f"{os.path.basename(project_dir)}_manifest.json")
        shutil.copy(manifest_path, dest_path)
        print(f"Collected {manifest_path} to {dest_path}")
    else:
        print(f"manifest.json not found in {project_dir}")

def main():
    for subproject in subprojects:
        project_dir = os.path.join(base_dir, subproject)
        run_dbt_compile(project_dir)
        collect_manifest(project_dir)

if __name__ == "__main__":
    main()
