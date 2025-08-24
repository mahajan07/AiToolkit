# 🐍 Python Virtual Environment & Environment Setup Guide


This document provides an all-in-one reference for setting up and managing Python virtual environments, environment variables, working with packages, and tips for VS Code.

---

## 🚀 1. Create and Activate Virtual Environment

```bash
# Check Python version
python --version

# Create virtual environment in folder `.venv`
python -m venv .venv

# Activate virtual environment

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Windows CMD
.venv\Scripts\activate.bat

# macOS / Linux
source .venv/bin/activate

# Deactivate virtual environment
deactivate
---
```
## 📦 2. Install Packages & Manage Dependencies
```bash
# Install dependencies from requirements.txt
pip install -r requirements.txt

# Save installed packages to requirements.txt
pip freeze > requirements.txt

# Upgrade pip
python -m pip install --upgrade pip

# Show package version
pip show package_name

# List installed packages
pip list

# Uninstall a package
pip uninstall package_name
```
