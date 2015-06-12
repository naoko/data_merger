data_merger
-----

data merger "join" files based on given meta file and output more readable data for data analysis

sample data under data directory was retrieved from [data.gov](http://catalog.data.gov/dataset/fedscope-employment-cubes)

the data set provides employee population data as of September 2009
each column value is separated by a "comma"
there is a main data (FACTDATA_SEP2009.TXT) and "dimension translations files"
for instance, data/DTloc.txt provides translations for the location data element contained in main data

for example, main data has column name "LOC" and its value is 06.
in DTloc.txt, there are columns called "LOC" and "LOCT". 
sample value of "LOC" would be 06 and "LOCT" would be "06-CALIFORNIA"

as output, you want "06-CALIFORNIA" instead of 06 as part of main data
to do so in metadata, you specify mapping relationship as follows 

    mapping_files:
    location: 
      file_name: "DTloc.txt"
      key: "LOC"
      value: "LOCT"

then it will match "LOC" value and map to value in "LOCT"


inspired by design/pattern below:

- strategy pattern: to provide various file type to read metadata 
- class registry: to reverse lookups which enable clean validation for factory
- custom exception: allow you to provide extra context
- [declarative programming](http://en.wikipedia.org/wiki/Declarative_programming): to avoid unnecessary hard-code

TODO:

- add more tests!
- add more error handling
- receive job via REST (use [EVE](http://python-eve.org/))
    - payload should include metadata and path to output so that i can remove hardcoded source/destination :(
- connect to custom data source such such as S3
- sphinx doc
- docker file for easier stand up


####get it started
    
    $ virtualenv data_merger_venv
    $ source data_merger_venv/bin/activate
    $ cd data_merger
    $ pip install -r requirements.txt
    
    # to process sample data
    # step 1. uncompress archive.zip and copy data to data_merger/data
    $ cd data_merger
    $ mkdir data_out
    $ python merger.py


###run test with intuitive test runner pytest

    $ cd data_merger
    $ py.test --cov .
