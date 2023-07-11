import json

"""
Run from root of repo:
python scripts/models_depth.py
"""
# open and loaf manifest file

with open('target/manifest.json') as f:
    manifest = json.load(f)

def count_models(node_list):
    c = 0
    for i in node_list:
        if i.startswith("model."):
            c += 1
    return c

table = []
for i in manifest["nodes"]:
    node = manifest["nodes"][i]
    if node["resource_type"] == "model":
        including_sources = len(node["depends_on"]["nodes"])
        excluding_sources = count_models(node["depends_on"]["nodes"])
        table.append([f"{node['schema']}.{node['name']}",including_sources,excluding_sources])

for i in sorted(table,key=lambda x: x[2],reverse=True):
    print(f"{i[0]},{i[1]},{i[2]}")