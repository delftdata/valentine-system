# Valentine Experiment Suite: Dataset Generator

This repository contains the dataset generator used by the Valentine Experiment Suite.  
For extended details please refer to the paper.

## Prerequisites

To run the generator you need:
- Python 3.8.x

## Dataset Generator

The Dataset Generator was implemented from scratch to address the needs of Valentine. It takes as input a given table and it
outputs two tables, derived from the horizontal/vertical partition of the first.  As explained in the corresponding
paper, the Dataset Generator aims to create unionable, view-unionable, joinable and semantically joinable pairs of tables
that serve as datasets upon which the experments of Valentine are conducted. The strategies and the techniques used from
the generator are on the same spirit with the existing works. They are presented in details in the aforementioned paper. 

## Transformation Options

The generator gives to the user the following transformation options:

- Horizontal split: The user can partition horizontally the input table in two new tables and she can choose
the percentage of row overlap. It can be used in combination with the vertical split.
- Vertical split: The user can partition vertically the input table in two new tables and she can choose
the percentage of column overlap. It can be used in combination with the horizontal split.
- Noise in Schema: The user can choose to add noise in the column names of the generated tables. The added noise is
is in the form of:
   - prefixing the column name with the table name,
   - abbreviating the column names,
   - dropping the vowels from the column names,
   - a combination of the above.
- Noise in Data: The user can choose to add noise in the data of the generated tables. To add noise in the columns
with strings, typos, based on keyboard proximity, are inserted in the values of the overlapped rows. For the numerical
only columns, the assumption that values follow a half normal distribution is made. Based on that the values are recomputed
after randomly changing a bit the mean and the standard deviation of the distribution. 

## Output

The output of the generator consists of:
- a json file containing the mapping between the two tables.
- a json file containing the schema of the first (suffixed as source) table.
- a csv file containing the data of the first (suffixed as source) table, including a header with the column names.
- a json file containing the schema of the second (suffixed as target) table.
- a csv file containing the data of the second (suffixed as target) table, including a header with the column names.

## Execution Instructions

To run the generator the following steps should be followed:
- Create a target and a source folder, named accordingly.
- In the provided config file:
     - Change the home directory to yours (please provide the absolute path!).
     - Change the source_data and source_schema attributes to match your input file. As input files the generator expects
     a csv file containing the data (without the column names as a header) and a csv file containing the schema (in the form
     of "column_name,type" ).
     - Change the rest of the sections in order to choose from the above mentioned transformation option. In details:
         - From the Properties section set the values accordingly for splitting horizontally, vertically or in both ways.
         Set the the percentage attribute to an integer between 0 and 100 to specify the row overlap. Setting random_overlap
         to True will result in a random selection of the overlapped rows.
         - From the Columns section, the pk column can be specified with PK. If a random overlap of columns is desired set
         split_random to True. With the columns attribute the percentage of the column overlap can be selected.
         - From the Approximation section, the noise settings are controlled. For noise in data the approx attribute should
         be set to True. Through the percentage, the percentage of the overlapped rows, to which noise will be added, can
         be selected. The attribute approx_percentage controls the how much a value will be change. Approx_columns should be
         set to True for noise in schema and with the approx_columns_type the type of the added noise can be chosen
         (value in [1,5]).
- Run generator with the command: `python dataset_generator config.ini`. Depending on your python setup python3.8 or python3
might need to be used instead of python.


     
