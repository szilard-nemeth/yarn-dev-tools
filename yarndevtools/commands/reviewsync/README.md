# Hadoop reviewsync

This project is aimed to track if patches can be applied to specified branches from Jira issues having patches as attachments.

### Getting started / Setup

You need to have python 2.7 and pip installed.
Run make from the project's root directory, all python dependencies will be installed required by the project.


## Running the tests

TODO

## Main dependencies

* [jira](https://jira.readthedocs.io/en/master/) - Python JIRA: Python library to work with JIRA APIs
* [gitpython](https://gitpython.readthedocs.io/en/stable/) - GitPython is a python library used to interact with git repositories, high-level like git-porcelain, or low-level like git-plumbing.
* [gspread](https://gspread.readthedocs.io/en/latest/) - gspread is a Python API for Google Sheets
* [tabulate](https://pypi.org/project/tabulate/) - python-tabulate: Pretty-print tabular data in Python, a library and a command-line utility.
* [oauth2client](https://oauth2client.readthedocs.io/en/latest/) - oauth2client: Used to authenticate with Google Sheets

## Contributing

TODO 

## Authors

* **Szilard Nemeth** - *Initial work* - [Szilard Nemeth](https://github.com/szilard-nemeth)

## License

TODO 

## Acknowledgments

TODO


## Example commands

1. Check if patches can be applied to trunk and branch-3.2 downloaded from the specified Jira issues
```
python ./reviewsync/reviewsync.py -i YARN-9138 YARN-9139 -b branch-3.2 -v
```

2. Check if patches can be applied to trunk, branch-3.2 and branch-3.1 downloaded from the Jira issues found in Google Sheet.
If --gsheet is specified, a number of other Google Sheet specific arguments are required: 
  * --gsheet-client-secret: File to be used for authenticate to Google Sheets API.
  * --gsheet-spreadsheet: Name of the spreadsheet (document) on Google Sheet.
  * --gsheet-worksheet: The name of the worksheet from the spreadsheet (document).
  * --gsheet-jira-column: Column to look into, in order to find JIRA issues.
  * --gsheet-update-column: The column to update with the updated date.
  * --gsheet-status-info-column: The column to update with the overall status of applying the patch.
  
```
python ./reviewsync/reviewsync.py --gsheet -b branch-3.2 branch-3.1 -v \
--gsheet-client-secret "/Users/szilardnemeth/.secret/client_secret_hadoopreviewsync.json" \ 
--gsheet-spreadsheet "YARN/MR Reviews" --gsheet-worksheet "Incoming" --gsheet-jira-column "JIRA" \ 
--gsheet-update-date-column "Last Updated" --gsheet-status-info-column "Reviewsync"
```