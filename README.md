# MIDAS Data Viewer

## Installation

1. Install Rust v1.77.1 or above. 
   * If you don't have Rust installed, follow the directions [here](https://www.rust-lang.org/tools/install).
   * If you already have Rust installed, make sure you're on at least 1.77.1 by running `rustup upgrade`.
2. Install the MIDAS Data Viewer locally using `cargo install --git https://github.com/redindelible/MIDAS-Launch`. This builds a local copy of the resulting executable.
3. Run the Data Viewer executable by typing `midas-launch` into your command line.

## Usage
The workflow for using this software can be divided into 4 steps:
1. Load data from either a raw launch file or from a CSV.
2. (Possibly) Apply processing steps and filters.
3. View the data in a tabular or graphical form.
4. (Possibly) Reexport the data to a CSV.

### Loading data from CSVs
   
Loading data from CSVs is fairly simple. Select the 'Import' tab, then select the source type
'.csv File'. Now either paste in the absolute path to the CSV file you wish to load, or click
'Choose File' and select the file you wish to load in.

Once you've chosen a file, click the 'Load Data' button. If all goes well, the data will be
loaded into the table in a few seconds.

### Loading data from .launch files

Loading data from .launch files is much more complicated. To begin, select the 'Import' tab and select
the source type of '.launch File'.

First, click the 'Choose File' button under the 'Data File' header and choose the .launch file
you want to load. The program will attempt to store the last file you loaded across runs, so
if you're loading the same file again this step can be skipped.

Now, you have to tell the program the format that this .launch file was encoded in. This takes some 
extra steps to set up:

1. Download a __new__ copy of the MIDAS-Software repository somewhere on your computer.
2. Note the git hash of the commit that was flashed to MIDAS. Use
`git reset --hard <hash>` with `<hash>` replaced with the noted hash to revert the above
repository to the correct version.
3. Install Python 3.9 or above. On Windows, make sure you check the box that automatically adds
Python to your 'PATH' environment variable. Make sure Python can be invoked from the command line.
4. Install the module `lark` for your Python 3.9 installation using `pip install lark`. 

Now that you've followed the above steps, click on the 'Choose File' button under the 'Data Format'
header and navigate to the copy of the MIDAS-Software above. Navigate into the repository until you
find the `log_format.h` file and select it (it will be found at `MIDAS-Software/MIDAS/src/log_format.h`).

For the 'Python Command' field, enter the command to invoke the Python 3.9 installation. This will
be autofilled with `python`, which will likely work on Windows if the steps above were followed. On MacOS
and Linux, this will most likely have to be replaced with `python3` or `python3.9` since the default `python`
command is for Python 2.7. 

Next, click the 'Load Format' button. If all goes well, this should parse the format and display the checksum
of the format next to the 'Data Format' header. If you want to check that the launch data file
was created by the same format, click the 'Inspect Source' button and verify that the checksum is the same.

Now, you can click the 'Load Data' button. In a few seconds, this will have imported all the data
from the launch file.

### Processing

This software provides 4 different types of filters. You can add any number of filters,
and you may add each filter any number of times in any order. You can click the '-' button to
remove the associated row, '^' to swap it with the one above, or 'v' to swap it with the one below.

> [!NOTE]
> Each time you click 'Apply', the filters will all be run again from a 
> clean, unprocessed copy of the data.

* Fill: Fills in the empty cells with the contents of either the previous (for downwards) 
or next (for upwards) non-empty cell. If 'Backfill' is selected, then the empty cells at the start
and ends of the table are also filled with the closest non-empty cell.
* Select: Only retain the rows of the table where the value of the chosen column of that row is equal
to the provided value.
* Within: Only retain the rows of the table where the value of the chosen column match the conditions: If 'Lower' is selected,
then only rows with a value above the provided bound are retained. If 'Upper' is selected, then only rows
with a value below the provided are retained. Both can be selected at the same time.
* Sort: Sort the rows of the table by the value of the chosen column, in either ascending or descending order.

### Plotting