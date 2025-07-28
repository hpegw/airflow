import sys
import subprocess
import importlib.util

# List of required packages
required_packages = [
    "requests"
]
 
# Check if each package is installed, install if missing
for package in required_packages:
    # Check if the package is already installed
    if importlib.util.find_spec(package) is None:
        print(f"{package} not found, attempting to install...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install {package}: {e}")
            print("Please install the required packages manually and retry.")
            sys.exit(1)
