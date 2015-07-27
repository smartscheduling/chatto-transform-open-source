# chatto-transform
Feature generation/flat format management

# Installation

- Download Miniconda: http://conda.pydata.org/miniconda.html
NOTE: Use the link for Python 3.4, NOT 2.7.

- Run the conda installation script in the command line:

  `bash Miniconda-latest-Linux-x86_64.sh`

- Clone this repository:

  `git clone git@github.com:smartscheduling/chatto-transform-open-source.git`

- Install the dependencies. `cd` into the top-level directory of the repo, and run:

  `conda install chatto_transform_dependencies-0-np19_0.tar.bz2`

- Add chatto-transform to your PYTHONPATH environment variable. Edit your .bashrc file or equivalent, and add the line:

  `export PYTHONPATH='/Path/to/chatto-transform:$PYTHONPATH'`

  Save it, and then run the file in the command line:

  `source .bashrc`

# Run the Jupyter Notebook

- `cd` into the `notebooks/` directory

- Run: `ipython notebook`

- A browser window should open with access to all available notebooks.