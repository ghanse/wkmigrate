import os

# Get the project root directory (go up from src/wkmigrate/ to the project root)
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# Path to test JSON files
JSON_PATH = os.path.join(_project_root, "tests", "resources", "json")

# Path to test YAML files
YAML_PATH = os.path.join(_project_root, "tests", "resources", "yaml")