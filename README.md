RadomGTFS
=========

Description
-----------

Prettifies and merges GTFS files for Radom, based on [data published by MZDiK](https://mzdik.pl/index.php?id=145).


Running
-------

RadomGTFS is written in Python with the [Impuls framework](https://github.com/MKuranowski/Impuls).

To set up the project, run:

```terminal
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.txt
```

Then, run:

```terminal
$ python radom_gtfs.py
```

The resulting schedules will be put in a file called `radom.zip`.

License
-------

_RadomGTFS_ is provided under the MIT license, included in the `LICENSE` file.
