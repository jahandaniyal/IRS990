try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import boto
import csv
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import sys

def getDataFromXML(URI):
    #tree = ET.ElementTree(file='201541349349307794_public.xml')
    tree = ET.ElementTree(ET.fromstring(URI))
    namespace="//{http://www.irs.gov/efile}"
    irs = tree.find(namespace+"IRS990")
    irs_ez = tree.find(namespace + "IRS990EZ")
    filer = tree.find(namespace+"Filer")
    xmldata = 'NULL'
    ein='NULL'
    for item in filer:
        #print(item.tag, item.text)
        if 'ein' in item.tag.lower():
            ein = item.text
            #print(item.text)
        elif 'usa' in item.tag.lower():
            xmldata = item[2].text if len(item[2].text)==2 else item[3].text
            #print(item[2].text)
    if irs:
        for item in irs:
            if 'totalrevenue' in item.tag.lower() and 'c' in item.tag.lower():
                xmldata += ',' + item.text
                break
    elif irs_ez:
        for item in irs_ez:
            if 'totalrevenue' in item.tag.lower():
                xmldata += ',' + item.text
                break
    return [ein,xmldata]

def getCsv(partition):
    conn = boto.connect_s3(host="s3.amazonaws.com").get_bucket("irs-form-990")
    try:
        for row in partition:
            xml = conn.get_key(str(row)).get_contents_as_string()
            return_data = getDataFromXML(xml)
    except:
        return [str(row),'NULL, NULL']
    return return_data

def getCsvForPartition(partition):
    conn = boto.connect_s3(host="s3.amazonaws.com").get_bucket("irs-form-990")
    return_data = []
    try:
        for row in partition:
            xml = conn.get_key(str(row[0])).get_contents_as_string()
            return_data.append(getDataFromXML(xml))
    except:
        return_data.append([str(row[0]),'NULL, NULL'])
    return return_data

if __name__ == "__main__":
    if len(sys.argv) != 4:
        exit(-1)
    sc = SparkContext(appName="SparkParseIRS")
    partitions = int(sys.argv[3])
    spark = SparkSession(sc)
    indexcsv = spark.read.csv(sys.argv[1], header=True)
    #o_ = indexcsv.rdd.map(lambda a: getCsv(a)).reduceByKey(lambda x, y: x)
    o_ = indexcsv.rdd.repartition(partitions).mapPartitions(lambda a:getCsvForPartition(a)).reduceByKey(lambda x,y : x)
    o_.saveAsTextFile(sys.argv[2])
    sc.stop()

