# Boosted HH utilities


[![Actions Status][actions-badge]][actions-link]
[![Codestyle](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/LPC-HH/boostedhh/main.svg)](https://results.pre-commit.ci/latest/github/LPC-HH/boostedhh/main)
<!-- [![Documentation Status][rtd-badge]][rtd-link] -->

<!-- SPHINX-START -->

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/LPC-HH/boostedhh/workflows/CI/badge.svg
[actions-link]:             https://github.com/LPC-HH/boostedhh/actions
[conda-badge]:              https://img.shields.io/conda/vn/conda-forge/boostedhh
[conda-link]:               https://github.com/conda-forge/boostedhh-feedstock
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/LPC-HH/boostedhh/discussions
[pypi-link]:                https://pypi.org/project/boostedhh/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/boostedhh
[pypi-version]:             https://img.shields.io/pypi/v/boostedhh
[rtd-badge]:                https://readthedocs.org/projects/boostedhh/badge/?version=latest
[rtd-link]:                 https://boostedhh.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->

Common code and utilities for boosted HH analyses.


- [Boosted HH utilities](#boosted-hh-utilities)
  - [Setting up package](#setting-up-package)
    - [Creating a virtual environment](#creating-a-virtual-environment)
    - [Installing package](#installing-package)
    - [Troubleshooting](#troubleshooting)


## Setting up package

### Creating a virtual environment

First, create a virtual environment (`micromamba` is recommended):

```bash
# Download the micromamba setup script (change if needed for your machine https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html)
# Install: (the micromamba directory can end up taking O(1-10GB) so make sure the directory you're using allows that quota)
"${SHELL}" <(curl -L micro.mamba.pm/install.sh)
# You may need to restart your shell
micromamba env create -f environment.yaml
micromamba activate hh
```

### Installing package

**Remember to install this in your mamba environment**.

```bash
# Clone the repository
git clone https://github.com/LPC-HH/boostedhh.git
cd boostedhh
# Perform an editable installation
pip install -e .
# for committing to the repository
pip install pre-commit
pre-commit install
```

### Troubleshooting

- If your default `python` in your environment is not Python 3, make sure to use
  `pip3` and `python3` commands instead.

- You may also need to upgrade `pip` to perform the editable installation:

```bash
python3 -m pip install -e .
```
