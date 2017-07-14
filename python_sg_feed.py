#!/usr/bin/python
import sys
sys.path.insert(0, r'/usr/local/lib/python2.7/site-packages/')
import requests #Python Request libary for handling streaming HTTP link is here: http://docs.python-requests.org/en/master/

url_param = "?feed=continuous&include_docs=true&since=0"

r = requests.get('http://admin:pass@localhost:4984/todo/_changes'+url_param, stream=True)

for line in r.iter_lines():
	if line:
		print line
'''
### FILTER BY CHANNELS ###
channels = 'bob,water,cake'
url_param = url_param +'&filter=sync_gateway/bychannel&channels='+channels
'''

'''
### Saving My App's Checkpoint inside Sync Gateway ###
HTTP/1.1 PUT or GET
Content-Type: application/json   
http://{hostname}:4984/{DB}/_local/my_check_point_{ip address of app}
{
	"_id":"my_check_point_8.8.8.8",
	"_rev":"1-0",
	"datetime":"2016-12-04 17:34:57",
	"seq":"5"	
}
Docs on local docs: https://developer.couchbase.com/documentation/mobile/1.4/references/sync-gateway/rest-api/index.html#/document
'''
