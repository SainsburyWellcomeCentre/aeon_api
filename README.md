# aeon_api

![swc-aeon](https://github.com/SainsburyWellcomeCentre/aeon_api/actions/workflows/swc-aeon.yml/badge.svg?branch=main)
[![swc-aeon_code_coverage](https://codecov.io/gh/SainsburyWellcomeCentre/aeon_api/branch/main/graph/badge.svg?token=973EC1CG03)](https://codecov.io/gh/SainsburyWellcomeCentre/aeon_api)

Project Aeon low-level library for interfacing with acquired data. Contains modules for loading and processing raw data.

## Set-up Instructions

We recommend [uv](https://docs.astral.sh/uv/) for python version, environment, and package dependency management. However, any other tool compatible with the `pyproject.toml` standard should work.

### Install from PyPI

```
uv pip install swc-aeon
```

### Install from GitHub

```
uv pip install git+https://github.com/SainsburyWellcomeCentre/aeon_api
```

### Set up a development environment

```
git clone https://github.com/SainsburyWellcomeCentre/aeon_api
cd aeon_api
uv sync
```

This creates a `.venv` with the required runtime dependencies and the development tools, including the test runner, the linter, and the type checker.
Because running commands with `uv run` (e.g. `uv run pytest`) keeps the environment synchronised automatically, `uv sync` is only needed once during the initial setup.

To set up the pre-commit hooks:

```
uv run pre-commit install
```

## Repository Contents

- `.github/workflows/` : GitHub actions workflows for building the environment and running tests
- `src/swc/aeon/` : Source code for the Aeon Python package
    - `src/swc/aeon/analysis`: Source code for processing and plotting the raw data
    - `src/swc/aeon/io`: Source code for loading raw data
    - `src/swc/aeon/schema`: Core modules for defining data schemas used to load raw data from experiments
- `tests/` : Tests for the Aeon Python package
    - `tests/data` : Data used by tests
    - `tests/schema` : Schemas used to load sample data in tests
    - `tests/test_integration` : Integration tests
    - `tests/test_unit` : Unit tests, mirroring `src/swc/aeon/` package structure

## Citation Policy

If you use this software, please cite it as below:

D. Campagner, J. Bhagat, G. Lopes, L. Calcaterra, A. G. Pouget, A. Almeida, T. T. Nguyen, C. H. Lo, T. Ryan, B. Cruz, F. J. Carvalho, Z. Li, A. Erskine, J. Rapela, O. Folsz, M. Marin, J. Ahn, S. Nierwetberg, S. C. Lenzi, J. D. S. Reggiani, SGEN group – SWC GCNU Experimental Neuroethology Group. _Aeon: an open-source platform to study the neural basis of ethological behaviours over naturalistic timescales._ Preprint at https://doi.org/10.1101/2025.07.31.664513 (2025)

[![DOI:10.1101/2025.07.31.664513](https://img.shields.io/badge/DOI-10.1101%2F2025.07.31.664513-AE363B.svg)](https://doi.org/10.1101/2025.07.31.664513)
