#!/usr/bin/python
import requests #Python Request libary for handling streaming HTTP link is here: http://docs.python-requests.org/en/master/

secure = False  # Set to True for https (required for Couchbase Capella)
host = "localhost"
port = "4984"
sgDb = "db"
sgScope = "us"
sgCollection = "prices"
username = "bob"
password = "password"

url_param = "?feed=continuous&include_docs=true&since=0"

### FILTER BY CHANNELS ###
#channels = 'bob,water,cake'
#url_param = url_param +'&filter=sync_gateway/bychannel&channels='+channels

### Active Only ###
#url_param = url_param + '&active_only=true' #feed will NOT send `_deleted` docs

###docs on _change feed options https://docs.couchbase.com/sync-gateway/current/rest-api/rest_api_public.html#tag/Document/operation/get_keyspace-_changes

protocol = 'https' if secure else 'http'
r = requests.get(protocol+'://'+host+':'+port+'/'+sgDb+'.'+sgScope+'.'+sgCollection+'/_changes'+url_param, auth=(username, password), stream=True)

for line in r.iter_lines():
	if line:
		print(line)