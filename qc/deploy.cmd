@echo off
where uv >nul 2>&1 || powershell -ExecutionPolicy Bypass -Command "irm https://astral.sh/uv/install.ps1 | iex"
uv venv
uv sync --all-extras
