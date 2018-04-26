import json
import boto
from helper import *
from boto.s3.key import Key
from boto.s3.connection import S3Connection

print('Start Script')
r = boto.connect_s3(host="s3.amazonaws.com") \
            .get_bucket("irs-form-990") \
            .get_key("index_%i.json" % 2011) \
            .get_contents_as_string()
            #.replace("\r", "")
j = json.loads(r)
indexList = j[list(j)[0]]

outputFile = open("filer_data.csv", "w" )
csvWriter = csv.writer(outputFile, quoting=csv.QUOTE_MINIMAL)
csvWriter.writerow(['EIN', 'FILER', 'YEAR', 'REVENUE', 'STATE', 'CITY'])

count = 0
for item in indexList:

    csvWriter.writerow(get_filer_data(item['URL']))
    count +=1
    print(count)
print('End of Script!')