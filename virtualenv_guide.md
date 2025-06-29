# Python Virtual Environment & Terminal Commands Guide

This guide contains essential commands and snippets for creating, managing, and using Python virtual environments, working with environment variables, and helpful tips for VS Code workflows.

---

## Virtual Environment Setup & Activation

```bash
# Check Python version
python --version

# Create a virtual environment named '.venv' (Windows / macOS / Linux)
python -m venv .venv
bash
Copy
Edit
# Activate virtual environment

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Windows CMD
.venv\Scripts\activate.bat

# macOS / Linux
source .venv/bin/activate
bash
Copy
Edit
# Deactivate virtual environment
deactivate
Dependency Management
bash
Copy
Edit
# Install packages from requirements.txt
pip install -r requirements.txt

# Save current packages to requirements.txt
pip freeze > requirements.txt

# Upgrade pip
python -m pip install --upgrade pip
Working with Packages
bash
Copy
Edit
# Show installed package version
pip show package_name

# List all installed packages
pip list

# Uninstall a package
pip uninstall package_name
Environment Variables
bash
Copy
Edit
# Print env variable

# Windows CMD / PowerShell
echo %VARIABLE_NAME%

# macOS / Linux
echo $VARIABLE_NAME
bash
Copy
Edit
# Temporarily set env variable (session only)

# Windows PowerShell
$env:VARIABLE_NAME="value"

# Windows CMD
set VARIABLE_NAME=value

# macOS / Linux
export VARIABLE_NAME="value"
Running Python Scripts
bash
Copy
Edit
python script.py
# or
python3 script.py
VS Code Tips
Select Python interpreter:
Ctrl + Shift + P → Search: Python: Select Interpreter → Choose .venv interpreter

Restart VS Code terminal:
Shortcut Ctrl + ` (backtick)

Recommended .gitignore for Python Projects
gitignore
Copy
Edit
# Virtual environment folder
.venv/

# Python cache files
__pycache__/
*.py[cod]

# Environment variables file
.env

# Jupyter Notebook checkpoints
.ipynb_checkpoints/
Useful Python Snippets for Environment & Paths
python
Copy
Edit
import os
from dotenv import load_dotenv

# Current script directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Project root (one folder up)
root_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Load .env file from project root
env_path = os.path.join(root_dir, '.env')
load_dotenv(env_path)
