#!/usr/bin/python
import sys
sys.path.insert(0, r'/usr/local/lib/python2.7/site-packages/')
import requests #Python Request libary for handling streaming HTTP link is here: http://docs.python-requests.org/en/master/

url_param = "?feed=continuous&include_docs=true&since=0"

r = requests.get('http://localhost:4984/sync_gateway/_changes'+url_param, stream=True)

for line in r.iter_lines():
	if line:
		print line
'''
### FILTER BY CHANNELS ###
channels = 'bob,water'
url_param = url_param +'&filter=sync_gateway/bychannel&channels='+channels
'''
