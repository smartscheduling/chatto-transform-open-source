# chatto-transform

Chatto-transform is an open source library for connecting to data sources and transforming data. It focuses on connecting the user to the data with as little fuss as possible, and providing ways to collaboratively build up dataset-specific transformations over time.

Chatto-transform stands on the shoulders of giants- it uses the Pandas library for in-memory data representation, and SQLAlchemy for connecting to SQL databases. Part of the core philosophy it embraces is that with modern computers it is entirely feasible and extremely convenient to perform analysis and transforms in memory, using a powerful, general-purpose programming language like Python. And for most tasks, doing it this way is much easier than doing the same with SQL queries. If your data is not above terabyte or petabyte scales then using in-memory transformations with the right tools is a great alternative to SQL.

Chatto-transform introduces a few abstractions of its own, and users of the library are free to use all or none of them.

For instance, you can use it as a thin layer over a particular database to easily load the contents of tables into pandas DataFrames. The loading routines use the faster CSV-writing methods available with some databases, like PostgreSQL and MySQL. It falls back on more standard approaches when nothing faster is available. So loading tables into memory is quite fast.

You can also store this data, or transformation results, locally. This can speed up your workflow by saving you from having to reload data from a SQL database. Chatto-transform supports reading and writing CSVs, HDF5 files, files on Amazon S3, and more.

Finally, you can capture domain-specific information about your dataset using Chatto-transform's Schema and Transform abstractions. These abstractions are intended to get out of your way while letting you grow a set of useful transformations for your dataset organically.


# Installation

- Download Miniconda: http://conda.pydata.org/miniconda.html
NOTE: Use the link for Python 3.4, NOT 2.7.

- Run the conda installation script in the command line:

  `bash Miniconda-latest-Linux-x86_64.sh` (May be something other than linux depending on your operating system)

- Clone this repository:

  `git clone git@github.com:smartscheduling/chatto-transform-open-source.git`

- Install the dependencies. `cd` into the top-level directory of the repo, and run:

  `conda install chatto_transform_dependencies-0-np19_0.tar.bz2` and type 'y' when prompted.

- Add chatto-transform to your PYTHONPATH environment variable. Edit your .bashrc file or equivalent, and add the line:

  `export PYTHONPATH='/Path/to/chatto-transform:$PYTHONPATH'`

  NOTE: replace Path/to/chatto-transform with your actual path to the repo.

  Save it, and then run the file in the command line:

  `source .bashrc`

# Run the Jupyter Notebook

- `cd` into the `notebooks/` directory

- Run: `ipython notebook`

- A browser window should open with access to all available notebooks.


# Connect to the MIMIC database

You must have permission to access the MIMIC database already, and you should have a username and password for your login.

- Create a directory `config/` inside the chatto_transform subdirectory:

  `mkdir chatto_transform/config`

- Create a new file here called `mimic_config.py` and write your username and password as follows:

  ````
  username = 'your username'
  password = 'your password'
  ````

  NOTE: never let your username and password get online. Chatto-transform will not let you commit anything inside the `config/` directory to git. Make sure your password doesn't show up anywhere else in the code.

- Save it and you should be good to go. Before using any of the MIMIC transforms or trying to load mimic tables, remember to call `session.login()`.