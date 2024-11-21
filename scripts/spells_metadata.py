import sys
import subprocess
import json
import os
import requests
import yaml

def get_starting_line(filename):
    with open(filename, 'r') as file:
        for line_number, line in enumerate(file, 1):
            if "}}" in line and "}}'" not in line:
                return line_number
    return 1
                
def get_authors_loc(filename, starting_line):
    # get authors in git blame and omit config block
    cmd = ['git', 'blame', '--line-porcelain', filename, '-L', f'{starting_line}']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return [line[7:] for line in stdout.decode('utf-8').split('\n') if line.startswith('author ')]

def get_authors_commit_spellbook():
    # return list of all authors who have committed to the whole project
    cmd = ['git', 'log', '--format=%an']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return [line for line in stdout.decode('utf-8').split('\n') if line]

def get_authors_commit(filename):
    # return list of all authors who have committed to this file
    cmd = ['git', 'log', '--format=%an', filename]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return [line for line in stdout.decode('utf-8').split('\n') if line]

def get_all_commits():
    # return list of all commits
    # git log --pretty=format:commit,%at,%an --name-only
    cmd = ['git', 'log', '--pretty=format:commit,%h,%at,%an', '--name-only']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    csv_string = "hash,timestamp,author,filename\n"
    for line in stdout.decode('utf-8').split('\n'):
        if line.startswith('commit'):
            hash = line.split(',')[1]
            timestamp = line.split(',')[2]
            author = line.split(',')[3]
        elif line.endswith('.sql'):
            csv_string += f"{hash},{timestamp},{author},{line}\n"
    upload_csv(csv_string, "spellbook_commits")

def get_all_commits_stats():
    # return list of all commits
    # git log --pretty=format:commit,%at,%an -p 
    cmd = ['git', 'log', '--pretty=format:commit,%h,%at,%an', '--numstat']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    csv_string = "hash,timestamp,author,filename,lines_added,lines_removed\n"
    for line in stdout.decode('utf-8').split('\n'):
        if line.startswith('commit'):
            hash = line.split(',')[1]
            timestamp = line.split(',')[2]
            author = line.split(',')[3]
        else:
            stats = line.split('\t')
            if len(stats) == 3:
                if stats[2].endswith('.sql'):
                    csv_string += f"{hash},{timestamp},{author},{stats[2]},{stats[0]},{stats[1]}\n"
    # print(csv_string)
    upload_csv(csv_string, "spellbook_commits_stats")

def get_projects_and_sectors():
    # walk models/ and subdirectories and find all the yml files
    csv_string = "filename,sector,project\n"
    for root, dirs, files in os.walk("models/"):
        for file in files:
            if file.endswith(".yml"):
                # open yaml file and get project and sector
                with open(os.path.join(root, file), "r") as f:
                    data = yaml.load(f, Loader=yaml.FullLoader)
                    if "models" in data:
                        for model in data["models"]:
                            model_name = (model["name"])
                            sector = 'Null'
                            project = 'Null'
                            if "meta" in model:
                                if "sector" in model["meta"]:
                                    sector = model["meta"]["sector"]    
                                if "project" in model["meta"]:
                                    project = model["meta"]["project"]
                            csv_string += f"{root}/{model_name}.sql,{sector},{project}\n"
    upload_csv(csv_string, "spellbook_projects_and_sectors")

def main():
    with open("target/manifest.json", "r") as f:
        # print(f"Loading manifest file at {manifest_path} ...")
        manifest = json.load(f)
    authors = {}
    for node_name in manifest["nodes"]:
        node_data = manifest["nodes"][node_name]
        
        if node_data["resource_type"] == "model":
            schema = node_data["schema"]
            name = node_data["alias"]
            filename = node_data["original_file_path"]
        if not filename or not filename.startswith("models/"):
            # skip empty lines and non-model files
            continue

        if filename not in authors:
            # initialize author data for this spell
            authors[filename] = {}
            authors[filename]["filename"] = filename
            authors[filename]["name"] = name
            authors[filename]["schema"] = schema
            authors[filename]["loc"] = {}
            authors[filename]["commit"] = {}
        starting_line = get_starting_line(filename)
        for author in get_authors_loc(filename, starting_line):
            # count lines of code for each author in git blame
            if author in authors[filename]["loc"]:
                authors[filename]["loc"][author] += 1
            else:
                authors[filename]["loc"][author] = 1
        for author in get_authors_commit(filename):
            # count commits for each author in git log
            if author in authors[filename]["commit"]:
                authors[filename]["commit"][author] += 1
            else:
                authors[filename]["commit"][author] = 1
    
    return authors

def upload_csv(table_csv, target):
    """
    Upload CSV string to dune.

    target = name of table to upload csv to
    """

    print(f"Writing {target} to Dune.com ...")
    url = 'https://api.dune.com/api/v1/table/upload/csv'
    api_key = os.environ.get('DUNE_API_KEY_PROD')
    if not api_key:
        raise Exception('DUNE_API_KEY_PROD environment variable not set!')
    headers = {'X-Dune-Api-Key': api_key}
    payload = {
        "table_name": target,
        "description": "Tables generated by Spark Spellbook.",
        "data": table_csv
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    if response.status_code == 200 and response.json()['success']:
        print(f'Success writing CSV to dune.dune.dataset_{target} ', flush=True)
    else:
        print('Error writing CSV to Dune.com!')
        raise Exception(response.content)
    
def generate_table(type,data): 
    csv_string = f"filename,schema,name,author,{type}\n"
    for file in data:
        for author in data[file][type]:
            csv_string += f"{file},{data[file]['schema']},{data[file]['name']},{author},{data[file][type][author]}\n"
    upload_csv(csv_string, f"spells_metadata_{type}")

def generate_project_commit_file():
    authors = get_authors_commit_spellbook()
    author_count = {}
    for author in authors:
        if author in author_count:
            author_count[author] += 1
        else:
            author_count[author] = 1
    csv_string = "author,commit_count\n"
    for author in author_count:
        csv_string += f"{author},{author_count[author]}\n"
    upload_csv(csv_string, "spellbook_project_commit_count")

if __name__ == '__main__':
    authors = main()
    generate_table("loc",authors)
    generate_table("commit",authors)
    generate_project_commit_file()
    get_all_commits()
    get_all_commits_stats()
    get_projects_and_sectors()