import yaml
from pathlib import Path


_config = None

def get_config():
    """
    Loads the configurations from config/config.yml.
    Caches the results for subsequent calls.
    """
    global _config
    if _config is None:
        try:
            config_path = Path(__file__).parent.parent / 'config/config.yml'
            with open(config_path, 'r') as f:
                _config = yaml.safe_load(f)
        except FileNotFoundError:
            raise RuntimeError("Configuration file 'config/config.yml' not found.")
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML configuration file: {e}")
    return _config

# You can also define a convenience variable for direct import
config = get_config()