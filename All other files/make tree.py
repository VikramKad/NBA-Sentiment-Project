import os

def print_tree(startpath, ignore_dirs=None):
    if ignore_dirs is None:
        ignore_dirs = []
    for root, dirs, files in os.walk(startpath):
        # Filter out ignored directories
        dirs[:] = [d for d in dirs if d not in ignore_dirs]

        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        # Print the directory itself, but handle the root case separately for cleaner output
        if root == startpath:
            print(f'{os.path.basename(root)}/')
        else:
            print(f'{indent}{os.path.basename(root)}/')
        
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f'{subindent}{f}')

if __name__ == "__main__":
    # Get the current working directory (assumes you run it from the project root)
    project_root = os.getcwd() 
    # Directories to ignore
    ignored = ['.git', '__pycache__', '.vscode', '.idea'] 
    
    # Call print_tree starting from the project_root's subdirectories
    # The root itself is printed before the loop in print_tree when root == startpath
    # To avoid printing the root directory name twice, we will iterate its contents
    # directly or adjust the print_tree function logic.

    # A simpler approach for the main call, ensuring the root is printed once:
    print(f'{os.path.basename(project_root)}/')
    for item in os.listdir(project_root):
        item_path = os.path.join(project_root, item)
        if os.path.isdir(item_path):
            if item not in ignored:
                print_tree(item_path, ignore_dirs=ignored) # Pass full path
        else:
            # Print files directly under the root
            # The indentation for root files needs to be consistent
            print(f'    {item}') 

    # Note: The above modification to the __main__ block is a bit naive.
    # A more robust print_tree function would handle the root printing internally.
    # Let's refine print_tree and the main call.

# Refined script:

import os

def generate_tree(dir_path, prefix="", ignore_dirs=None):
    if ignore_dirs is None:
        ignore_dirs = []
    
    # Get directory contents
    try:
        contents = os.listdir(dir_path)
    except OSError:
        print(f"{prefix}└── [Error accessing {os.path.basename(dir_path)}/]")
        return

    # Filter out ignored directories/files from the contents list
    filtered_contents = [item for item in contents if item not in ignore_dirs]
    
    pointers = ['├── '] * (len(filtered_contents) - 1) + ['└── ']
    
    for pointer, item_name in zip(pointers, filtered_contents):
        item_path = os.path.join(dir_path, item_name)
        if os.path.isdir(item_path):
            print(f"{prefix}{pointer}{item_name}/")
            extension = '│   ' if pointer == '├── ' else '    '
            generate_tree(item_path, prefix + extension, ignore_dirs)
        else:
            print(f"{prefix}{pointer}{item_name}")

if __name__ == "__main__":
    project_root = os.getcwd()
    ignored = ['.git', '__pycache__', '.vscode', '.idea', '.DS_Store'] # Added .DS_Store
    
    print(f"{os.path.basename(project_root)}/")
    generate_tree(project_root, ignore_dirs=ignored)
