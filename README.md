![CI for yarndevfunc (pip)](https://github.com/szilard-nemeth/linux-env/workflows/CI%20for%20cloudera/yarn/python%20%5Byarndevfunc%5D%20(pip)/badge.svg)
[![codecov](https://codecov.io/gh/szilard-nemeth/linux-env/branch/master/graph/badge.svg)](https://codecov.io/gh/szilard-nemeth/linux-env)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/github/szilard-nemeth/linux-env.svg)](https://lgtm.com/projects/g/szilard-nemeth/linux-env/context:python)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![GitHub language count](https://img.shields.io/github/languages/count/szilard-nemeth/linux-env)

# TODO fix links above

# YARN dev functions

This project contains various developer helper scripts in order to simplify everyday tasks related to Git and Apache Hadoop YARN development.

### Getting started / Setup

You need to have python 3.8 and pip installed.
Run make from this directory and all python dependencies will be installed required by the project.


## Running the tests

TODO

## Main dependencies

* [gitpython](https://gitpython.readthedocs.io/en/stable/) - GitPython is a python library used to interact with git repositories, high-level like git-porcelain, or low-level like git-plumbing.
* [tabulate](https://pypi.org/project/tabulate/) - python-tabulate: Pretty-print tabular data in Python, a library and a command-line utility.
* [bs4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) - Beautiful Soup is a Python library for pulling data out of HTML and XML files.
## Contributing

TODO 

## Authors

* **Szilard Nemeth** - *Initial work* - [Szilard Nemeth](https://github.com/szilard-nemeth)

## License

TODO 

## Acknowledgments

TODO


## Example commands


## Setup of precommit

Configure precommit as described in this blogpost: https://ljvmiranda921.github.io/notebook/2018/06/21/precommits-using-black-and-flake8/
Commands:
1. Install precommit: `pip install pre-commit`
2. Make sure to add pre-commit to your path. For example, on a Mac system, pre-commit is installed here: 
   `$HOME/Library/Python/3.8/bin/pre-commit`.
2. Execute `pre-commit install` to install git hooks in your `.git/` directory.

## Troubleshooting

### Installation issues
In case you're facing a similar issue:
```
An error has occurred: InvalidManifestError: 
=====> /<userhome>/.cache/pre-commit/repoBP08UH/.pre-commit-hooks.yaml does not exist
Check the log at /<userhome>/.cache/pre-commit/pre-commit.log
```
, please run: `pre-commit autoupdate`
More info here: https://github.com/pre-commit/pre-commit/issues/577