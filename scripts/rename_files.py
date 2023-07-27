import os

def rename_sql_files(root_directory):
    for root, _, files in os.walk(root_directory):
        for filename in files:
            if filename.endswith('_legacy.sql'):
                old_path = os.path.join(root, filename)
                new_filename = filename.replace('_legacy', '')
                new_path = os.path.join(root, new_filename)
                
                # Check if the new filename already exists to avoid overwriting
                counter = 1
                while os.path.exists(new_path):
                    name, extension = os.path.splitext(new_filename)
                    new_filename = f"{name}_{counter}{extension}"
                    new_path = os.path.join(root, new_filename)
                    counter += 1
                
                os.rename(old_path, new_path)
                print(f"Renamed: {old_path} -> {new_path}")

if __name__ == "__main__":
    root_directory = "macros/"
    rename_sql_files(root_directory)