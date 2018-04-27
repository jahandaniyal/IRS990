# IRS990
Analyzing publicly available IRS990 dataset using Apache Spark and Amazon Web Services.

**STRUCTURE:**
1. The directory **Data** contains the original input files "index_????.csv" and the **processed index files** "filer_data_????.csv"
2. Output of the Map Reduce tasks are stored inside **Data** under the directory **Output**
3. All scripts are contained inside the directory **Python**
  
**Requirements:**
Please use the requirements.txt file to install all dependencies.

**USAGE:**
1. filter_index.py is used for parsing the index files to get a list of comma delimited values consisting of the XML URI which is used for extracting data from individual XML Forms using Apache Spark running on a AWS EMR Cluster.
2. test_script.py and helper.py are used for a proof-of-concept demonstration of parsing a single XML form from the IRS990 dataset. I used this script to test the functionality before implementing it using Apache Spark.
3. irs.py is the actual script which does all the work of ETL on the IRS990 dataset.

**Running irs.py on EMR:**
Create a bucket on Amazon S3 and transfer the directories Data and Python.
To run irs.py successfully you need to pass 4 arguments in the following order:
  1. Path of irs.py on your AWS S3 Bucket
  2. Path of the **processed index files** inside data/input directory
  3. Location of the output file
  4. An integer representing the number of partitions to be used during Map Reduce.
The output is saved to the location provided as argument to the irs.py script.

Please find the results of **Year Over Year Total Revenue Growth Nationally and by State** in the data/output directory.

The Microsoft Powerpoint Presentation "Analyzing IRS990 Dataset.pptx" discusses my approach and the results of my analysis.
