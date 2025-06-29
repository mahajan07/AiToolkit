# 📁 Environment & Directory Management in Python Projects

This guide documents best practices for:

- 🔐 Loading `.env` files securely
- 📁 Managing file/directory paths dynamically
- 🔄 Using environment variables across reusable and modular Python projects

---

## ✅ 1. Loading `.env` Files Dynamically

```python
import os
from os.path import dirname, abspath, join
from dotenv import load_dotenv

# Get the current file's directory (e.g., /project/src/utils/config.py)
current_dir = dirname(abspath(__file__))

# Go up one or more levels to project root
root_dir = dirname(current_dir)  # or dirname(dirname(current_dir)) for deeper files

# Path to .env file
env_path = join(root_dir, ".env")

# Load it
load_dotenv(env_path)
