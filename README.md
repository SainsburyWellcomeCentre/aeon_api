# aeon_api

![aeon_api_env_build_and_tests](https://github.com/SainsburyWellcomeCentre/aeon_api/actions/workflows/build_env_run_tests.yml/badge.svg?branch=main)
[![aeon_api_tests_code_coverage](https://codecov.io/gh/SainsburyWellcomeCentre/aeon_api/branch/main/graph/badge.svg?token=973EC1CG03)](https://codecov.io/gh/SainsburyWellcomeCentre/aeon_api)

Project Aeon low-level library for interfacing with acquired data. Contains modules for loading and processing raw data.

## Set-up Instructions

We recommend [uv](https://docs.astral.sh/uv/) for python version, environment, and package dependency management. However, any other tool compatible with the `pyproject.toml` standard should work.

### Install from PyPI

```
uv pip install swc-aeon
```

### Install from source

```
git clone https://github.com/SainsburyWellcomeCentre/aeon_api
cd aeon_api
uv sync --all-extras
```

## Repository Contents

- `.github/workflows/` : GitHub actions workflows for building the environment and running tests 
- `aeon/` : Source code for the Aeon Python package 
    - `aeon/analysis`: Source code for processing and plotting the raw data
    - `aeon/io`: Source code for loading raw data
    - `aeon/schema`: Core modules for defining data schemas used to load raw data from particular experiments
- `tests/` : API unit tests
    - `tests/data` : Data used by tests
    - `tests/io` : Unit tests for the low-level raw data access API.
    - `tests/schema` : Schemas used to load sample data in test functions.

## Citation Policy

If you use this software, please cite it as below:

Sainsbury Wellcome Centre Foraging Behaviour Working Group. (2023). Aeon: An open-source platform to study the neural basis of ethological behaviours over naturalistic timescales,  https://doi.org/10.5281/zenodo.8411157

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8411157.svg)](https://zenodo.org/doi/10.5281/zenodo.8411157)
