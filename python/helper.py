import csv
import boto
import json
import xmltodict

def write_to_csv(filingData, file):

    with open(file, 'w', newline = '') as of:
        csvWriter = csv.writer(of, delimeter=',', quoting=csv.QUOTE_MINIMAL)
        csvWriter.writerow(['EIN','FILER','YEAR','REVENUE','STATE','CITY'])
        csvWriter.writerow(get_filer_data(''))


def get_filer_xml(filingURL):
    URLlink = filingURL.split('irs-form-990/')[1]
    xmlData = boto.connect_s3(host="s3.amazonaws.com") \
        .get_bucket("irs-form-990") \
        .get_key(URLlink).get_contents_as_string()
    return xmlData

def get_filer_json(xmlData):
    jsonData = xmltodict.parse(xmlData)
    return jsonData

def get_filer_data(filingURL):
    filerData = []
    jsonData = get_filer_json(get_filer_xml(filingURL))

    filerData.append(jsonData['Return']['ReturnHeader']['Filer']['EIN'])
    filerData.append(jsonData['Return']['ReturnHeader']['Filer']['Name']['BusinessNameLine1'])
    filerData.append(jsonData['Return']['ReturnHeader']['TaxYear'])
    filerData.append(jsonData['Return']['ReturnData']['IRS990']['TotalRevenueCurrentYear'])
    filerData.append(jsonData['Return']['ReturnHeader']['Filer']['USAddress']['State'])
    filerData.append(jsonData['Return']['ReturnHeader']['Filer']['USAddress']['City'])
    return filerData