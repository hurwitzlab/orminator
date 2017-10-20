# imicrobe-python-orm
Scripts to generate SQLAlchemy ORM classes for the iMicrobe database.

## Requirements
Specify the appropriate SQLAlchemy database URI with an environment variable
called `IMICROBE_DB_URI`. For example:

```
$ export IMICROBE_DB_URI=mysql+pymysql://imicrobe:<password>@localhost/imicrobe
```

The `pymysql` driver will be installed automatically. Other drivers can be
installed with `pip` (or `conda`).

## Installation
Use a virtual environment for ultimate happiness.

Clone this repository and do an 'editable' install with pip. Then execute
`write_imicrobe_models` to generate ORM classes.

```
$ git clone git@github.com:hurwitzlab/imicrobe-python-orm.git
$ cd imicrobe-python-orm
$ source ~/venv/bin/activate  # or similar
(venv) $ pip install -e .
(venv) $ write_imicrobe_models -o imicrobe_model/models.py
```

The ORM classes defined by the generated file `imicrobe_model/models.py` will
be available for import by the interpreter. The `models.py` file can be edited
or regenerated at any time. The python interpreter will use the latest version
each time it is executed. It will not pick up changes to `models.py` while running
without some extra help.

## Test
Execute the `demonstration.py` script.

```
(venv) $ python imicrobe_model/demonstration.py
```
