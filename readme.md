# RadomGTFS

## Description
Creates GTFS data for [MZDiK Radom](http://www.mzdik.radom.pl/).
This script uses database dumps straight from [MZDiK's website](http://www.mzdik.radom.pl/index.php?id=145).

## Prerequisits
[Python3](https://www.python.org) (version 3.6 or later) is required with 4 additional libraries:
- [requests](https://pypi.org/project/requests/),
- [Beautiful Soup 4](https://pypi.org/project/beautifulsoup4/),
- [tzlocal](https://pypi.org/project/tzlocal/),
- [zeep](https://pypi.org/project/zeep/).

All python requirements can be installed with `pip3 install -U -r requirements.txt`.

In addition to all that this script will invoke command `mdb-export`.
Ensure that [mdbtools](https://github.com/brianb/mdbtools) are installed
on your system (`sudo apt install mdbtools`).

## Running
`python3 radomgtfs.py` automatically creates the GTFS file.

`python3 radomgtfs.py --help` will show available options.

## License
*RadomGTFS* is provided under the MIT license, included in the `license.md` file.
