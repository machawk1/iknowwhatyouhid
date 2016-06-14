#!/usr/bin/env python

import requests
import sys
from surt import surt
from multiprocessing.dummy import Pool as ThreadPool 
import argparse

# Reads a text file consisting of LF'd URI-Rs from Tweets, fetches the mementos from IA's 
#  cdx server, and spits out temporally ordered URI-Ms with corresponding status codes

filenameFullOfLinks = 'testURIs.txt'
CPUs = 8
UK = True # False for Canada

# UK start date (default)
CANstartDate = '2015080300000'
CANendDate = '20151105235959'
UKstartDate = '20150227000000'
UKendDate = '20150508235959'

startDate = UKstartDate
endDate = UKendDate
if not UK:
  startDate = CANstartDate
  endDate = CANendDate

# Feb 27-May 8 is UK
# Aug-Nov is Canadian

# Threaded fetch process that spits out URI-Ms and corresponding status codes for each 
#  URI-R passed in within a date range
def getMementosWorker(uri):
  uri = 'http://web.archive.org/cdx/search/cdx?url={0}&from={1}&to={2}'.format(uri, startDate, endDate)

  resp = requests.get(uri)
  cdxData = resp.content.strip().split('\n')
  
  buff = ''
  
  for idx, cdxLine in enumerate(cdxData):
    if len(cdxLine) == 0:
      break;

    cdxFields = cdxLine.split(' ')

    httpStatus = cdxFields[4]
    urim = 'http://web.archive.org/web/{0}/{1}'.format(cdxFields[1], cdxFields[2])
    buff += "{0} {1}\n".format(urim, httpStatus)
  print buff

def main():
  global CPUs
  global startDate, endDate, UK
  
  parser = argparse.ArgumentParser(description='Task 4, read URIs, fetch mementos\nExample: $ python uk ./path/to/urlsFile.txt scriptName.py')
  parser.add_argument('filePath', help="Path to the file with URIs")
  parser.add_argument('-o', '--outfile', help='Path for the output, otherwise stdout')
  aargs = parser.parse_args()
  
  country = "Canada"
  if UK:
    country = "UK"
    print "--------------------\nProcessing {0} results for {1}-{2}\n--------------------\n".format(country,startDate,endDate)
  
  with open(aargs.filePath, 'r') as linksFile:
    urls = linksFile.read().split('\n')
    
    pool = ThreadPool(CPUs)
    results = pool.map(getMementosWorker, urls)
    pool.close() 
    pool.join() 
    
    # OBSOLETE: less efficient than above.
    # Query the CDX API
    #processes = []
    #for link in links:
    #  process = multiprocessing.Process(target=getMementosWorker, args=(link,))
    #  processes.append(process)
    #  process.start()

if __name__ == '__main__':
  main()