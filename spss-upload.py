import os
import sys
import getopt
import getpass
import csv
import base64
import json
import requests
from savReaderWriter import SavReader, SavHeaderReader

from pprint import pprint

# configuration values
config = dict(
    username = '',
    password = '',
    inputfile = '',
    #the number of rows to upload per bulk operation
    blocksize = 10000,
    append = False,
    dbname = '',
    authheader = ''
    )
usage = 'python ' + os.path.basename(__file__) + ' -f <csv file to import> -u <username> [-b <# of records/update] [-d <dbname>] [-a]'

def parse_args(argv):
    '''
    parse through the argument list and update the config dict as appropriate
    '''
    try:
        opts, args = getopt.getopt(argv, "hf:b:d:au:vi", 
                                   ["help",
                                    "file=",
                                    "blocksize=",
                                    "dbname=",
                                    "append",
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
            config['inputfile'] = arg
        elif opt in ("-b", "--blocksize"):
            config['blocksize'] = int(arg)
        elif opt in ("-d", "--dbname"):
            config['dbname'] = arg
        elif opt in ("-a", "--append"):
            config['append'] = True
        elif opt in ("-u", "--username"):
            config['username'] = arg

def init_config():
    if config['inputfile'] == '':
        print usage
        sys.exit()
    if config['username'] == '':
        print usage
        sys.exit()
    #let's check if the user specified a DB name
    #if not name it after the input file
    if config['dbname'] == '':
        config['dbname'] = config['inputfile'].split('.')[0].lower()
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

def initialize_db():
    '''
    create the database

    this essentially does:
    curl -X PUT 'https://<username>.cloudant.com/<dbname>' -H 'Cookie: <authcookie>'
    '''
    r = requests.put(config['dburl'], headers = config['authheader'])
    if r.status_code == 412 and not config['append']:
        print 'The database "{0}" already exists. Use -a to add records'.format(config['dbname'])
        sys.exit()

    #make SPSSDocType view
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    requestdata = {"_id": '_design/doctype',
                   "views": {"doctype":
                             {"map":"function(doc){if(doc.SPSSDocType){{emit(doc.SPSSDocType,null);}}}"}}}
    r = requests.post(
        config['dburl'],
        headers = headers,
        data = json.dumps(requestdata)
    )


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

def process_header():
    '''
    read through the input file and do a bulk update for each <blocksize> rows
    '''
    headers = config['authheader']
    headers.update({'Content-type': 'application/json'})
    requesturl = config['dburl']+'/_design/doctype/_view/doctype?startkey="header"&endkey="header"'
    r = requests.get(
        requesturl,
        headers = headers
        )
    result= json.loads(r.text)
    with SavHeaderReader(config['inputfile'], ioLocale='en_US.ISO8859-1') as header:
        metadata = header.dataDictionary()
        metadata['SPSSDocType'] = 'header'
        config['varNames'] = metadata['varNames']
    if result['total_rows'] < 1:
        updatedb(metadata)

def process_body():
    with SavReader(config['inputfile'], ioLocale='en_US.ISO8859-1') as body:
        for line in body:
            document = dict(zip( config['varNames'], line))
            document['SPSSDocType'] = 'data'
            updatedb(document)

def main(argv):
    parse_args(argv)
    init_config()
    get_password()
    authenticate()
    initialize_db()
    process_header()
    process_body()

if __name__ == "__main__":
    main(sys.argv[1:])
