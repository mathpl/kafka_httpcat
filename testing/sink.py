#!/usr/bin/python

from bottle import route, run, post, response, request
import bottle
import pprint
import zlib

#bottle.Request.MEMFILE_MAX = 2000000000

@post('/api/put')
def opentsdb_sink():
    pprint.pprint(request.headers.__dict__)
    if 'Content-Encoding' in request.headers and request.headers['Content-Encoding'] == 'gzip':
        body = request.body.getvalue()
        if len(body) > 0:
            try:
                print zlib.decompress(body, 15+32 )
            except Exception, e:
                print e
                print body
                print "\n"
        else:
            print "Empty message"
    else:
        print request.body.getvalue()


    response.status = 204
    return None

run(host='localhost', port=4242, debug=False)
