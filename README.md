# orminator
Generate SQLAlchemy ORM classes for an existing database.

## Requirements
An existing database and the SQLAlchemy database URI to connect with it, for example:
```
mysql+pymysql://imicrobe:<password>@localhost/imicrobe
```

The `pymysql` driver will be installed automatically. Other drivers can be
installed with `pip` (or `conda`).

## Installation and Simple Usage
Use a virtual environment for ultimate happiness.

Clone this repository and do an 'editable' install with pip. Then execute
`write_models` to generate ORM classes.

```
$ git clone git@github.com:hurwitzlab/orminator.git
$ cd orminator
$ source ~/venv/bin/activate  # or similar
(venv) $ pip install -r requirements.txt
(venv) $ write_models \ 
  -o orminator/models.py \
  -u mysql+pymysql://imicrobe:<password>@localhost/imicrobe
```

The ORM classes defined by the generated file `orminator/models.py` will
be available for import by the interpreter. The `models.py` file can be edited
or regenerated at any time. The python interpreter will use the latest version
each time it is executed. It will not pick up changes to `models.py` while running
without some extra help.

## Test
Execute the `demonstration.py` script.

```
(venv) $ python orminator/demonstration.py
```

## Fancy Usage
Generate ORM classes in other projects with `write_models`, or extend the
`orminator.ModelWriter` class to customize the models.