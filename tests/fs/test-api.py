#!/usr/bin/python

import httplib
import urllib
import time
import json

adr = 'localhost:30002'
has_error = False

def error(msg, args=[]):
	global has_error
	has_error = True
	print msg % args

def post(path, data=None, getargs={}):
	con = httplib.HTTPConnection(adr)
	con.connect()

	if getargs:
		path += "?" + urllib.urlencode(getargs)

	con.request('POST', path, data)
	return con.getresponse().read()


def get(path, getargs={}):
	con = httplib.HTTPConnection(adr)
	con.connect()

	if getargs:
		path += "?" + urllib.urlencode(getargs)
	
	con.request('GET', path, None)
	return con.getresponse().read()


def head(path, getargs={}):
	con = httplib.HTTPConnection(adr)
	con.connect()

	if getargs:
		path += "?" + urllib.urlencode(getargs) 
	
	con.request('HEAD', path, None)
	return json.loads(con.getresponse().read())


def hasChild(head, childname):
	arr = head["Children"]
	for child in arr:
		if child["Name"] == str(childname):
			return True

	return False


print 'API TEST'
print '------------------------------------------'

post("/home", "", {"type": "application/directory"})

i = 1
post('/home/nitro', 'salut', {"type": "application/json"})
hdr = head('/home/nitro')
if hdr["MimeType"] != "application/json":
	error('%d) /home/nitro should have a mimetype equals to application/json but received %s\n', (i, hdr['Exists']))

i += 1
if not hdr['Exists']:
	error('%d) /home/nitro should exist, but received header exists=%s\n', (i, hdr['Exists']))

i += 1
if hdr['Size'] != 5:
	error('%d) /home/nitro should be of size 5, but received size=%d\n', (i, hdr['Size']))

i += 1
post('/home/nitro2', 'salut')
hdr = head('/home/')
if not hasChild(hdr, "nitro") or not hasChild(hdr, "nitro2"):
	error('%d) /home should have children nitro and nitro2, but received children = %s\n', (i, hdr['Children']))

i += 1
hdr = head('/')
if not hasChild(hdr, "home"):
	error('%d) /home should have children home, but received children = %s\n', (i, hdr['Children']))


i += 1
hdr = head('/home/test')
if hdr['Exists']:
	error("%d) /home/test shouldn't exist, but received header exists=%s\n", (i, hdr['Exists']))


i += 1
value = get('/home/nitro')
if value != 'salut':
	error("%d) /home/nitro data should be 'salut', received %s\n", (i, value))



if has_error:
	print "FAIL!"
else:
	print "PASS!"




for i in range(0, 100):
	fp = open('data.original', 'r')
	start = time.time()
	post('/home/nitrof'+str(i), fp.read(), {}) 
	print 'WRITE '+ str(time.time() - start)
	fp.close()


for i in range(0, 100):
	fp = open('data.after', 'w')
	start = time.time()
	data = get('/home/nitrof'+str(i), {})
	fp.write(data)
	print 'READ  '+ str(time.time() - start)
	fp.close()


