import os
import sys
import getopt
import getpass
import csv
import base64
import json
import yaml
import requests
from savReaderWriter import SavReader, SavHeaderReader, SavWriter

from pprint import pprint

# configuration values
config = dict(
    username = '',
    password = '',
    outputfile = '',
    dbname = '',
    authheader = ''
    )
usage = 'python ' + os.path.basename(__file__) + ' -f <sav file to create> -u <username> -d <dbname>'

def parse_args(argv):
    '''
    parse through the argument list and update the config dict as appropriate
    '''
    try:
        opts, args = getopt.getopt(argv, "hf:d:u:", 
                                   ["help",
                                    "file=",
                                    "dbname=",
                                    "username="
                                    ])
    except getopt.GetoptError:
        print usage
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print usage
            sys.exit()
        elif opt in ("-f", "--file"):
            config['outputfile'] = arg
        elif opt in ("-d", "--dbname"):
            config['dbname'] = arg
        elif opt in ("-u", "--username"):
            config['username'] = arg

def init_config():
    if config['outputfile'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        print usage
        sys.exit()
    if config['dbname'] == '':
        print usage
        sys.exit()

    config['baseurl'] = 'https://{0}.cloudant.com/'.format(config['username'])
    config['dburl'] = config['baseurl'] + config['dbname']

def get_password():
    config['password'] = getpass.getpass('Password for {0}:'.format(config["username"]))
        
def authenticate():
    '''
    Authenticate to the cloudant using username and password
    Get a session cookie and save it

    This essentially does:
    curl -X POST -i 'https://<username>.cloudant.com/_session' -H 'Content-type: application/x-www-form-urlencoded' -d 'name=<username>&password=<password>'
    '''
    header = {'Content-type': 'application/x-www-form-urlencoded'}
    url = config['baseurl'] + '_session'
    data = dict(name=config['username'],
                password=config['password'])
    response = requests.post(url, data = data, headers = header)
    if 'error' in response.json():
        if response.json()['error'] == 'forbidden':
            print response.json()['reason']
            sys.exit()
    config['authheader'] = {'Cookie': response.headers['set-cookie']}

def updatedb(requestdata):
    '''
    posts <requestdata> to the database as a bulk operation
    <requestdata> is expected to be a json file which consists of multiple documents
    the form of <requestdata> is:
    {'docs': [{<doc1>}, {doc2}, ... {docn}]}

    this essentially does:
    curl -X POST 'https://<username>.cloudant.com/<dbname>' -H 'Cookie: <authcookie>' -H 'Content-type: application/json' -d '<requestdata>'
    '''
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    r = requests.post(
        config['dburl'],
        headers = headers,
        data = json.dumps(requestdata, encoding='iso8859-1')
        )

def process_body():
    with SavReader(config['inputfile'], ioLocale='en_US.ISO8859-1') as body:
        for line in body:
            document = dict(
                SPSSDocType = 'data',
                dataline = line)
            updatedb(document)

def get_header():
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    r = requests.get(
        config['dburl'] + '/_design/doctype/_view/doctype?startkey="header"&endkey="header"&include_docs=true',
        headers = headers
        )
    header = json.loads(r.text)['rows'][0]['doc']
    return(header)
    
def get_body(header):
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    r = requests.get(
        config['dburl'] + '/_design/doctype/_view/doctype?startkey="data"&endkey="data"',
        headers = headers
        )
    ids = []
    for row in yaml.safe_load(r.text)['rows']:
         ids.append(row['id'])
    blocknumber = 0
    blocksize = 200
    with SavWriter(config['outputfile'],
                   header['varNames'],
                   header['varTypes'],
                   valueLabels = header['valueLabels'],
                   varSets = header['varSets'],
                   varAttributes = header['varAttributes'],
                   varRoles = header['varRoles'],
                   measureLevels = header['measureLevels'],
                   caseWeightVar = header['caseWeightVar'],
                   varLabels = header['varLabels'],
                   formats = header['formats'],
                   multRespDefs = header['multRespDefs'],
                   columnWidths = header['columnWidths'],
                   fileAttributes = header['fileAttributes'],
                   alignments = header['alignments'],
                   fileLabel = header['fileLabel'],
                   missingValues = header['missingValues']) as writer:
        while blocknumber*blocksize < len(ids):
            document = dict(
                keys = ids[blocknumber*blocksize:(blocknumber+1)*blocksize])
            blocknumber += 1
            r = requests.post(
                config['dburl'] + '/_all_docs?include_docs=true',
                headers = headers,
                data = json.dumps(document)
                )
            for row in yaml.safe_load(r.text)['rows']:
                orderedrow = []
                for varName in header['varNames']:
                    orderedrow.append(row['doc'][varName])
                writer.writerow(orderedrow)

def get_data():
    header = get_header()
    get_body(header)

def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()
    get_data()

if __name__ == "__main__":
    main(sys.argv[1:])
