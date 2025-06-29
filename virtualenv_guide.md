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
