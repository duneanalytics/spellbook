"""
reads all files with names ending with _legacy.sql in the models directory and adds tag "legacy" to the config
"""

import os
import sys
import yaml

from pathlib import Path

# walk models directory and open every file ending with _legacy.sql
for root, dirs, files in os.walk("models"):
    for file in files:
        if file.endswith("_legacy.sql"):
            path = os.path.join(root, file)
            # open file and read the first line

            with open(path, "r") as f:
                content = f.read()

            if content.count("tags") != 1:
                print(content.count("tags"), path)
             
            if "'legacy'" not in content:
                if "tags=[" in content:
                    content = content.replace("tags=[", "tags=['legacy', ")
                else:
                    content = content.replace("config(", "config(\n\ttags=['legacy'],\n\t")
            
            
            # write the first line back to the file
            with open(path, "w") as f:
                f.write(content)

                
