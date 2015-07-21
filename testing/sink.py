#!/usr/bin/python

from bottle import route, run, post, response, request
import bottle
from zlib import decompress

#bottle.Request.MEMFILE_MAX = 2000000000

@post('/api/put')
def opentsdb_sink():
    if 'Content-Type' in request.headers and request.headers['Content-Type'] == 'gzip':
        body = request.body.getvalue()
        if len(body) > 0:
            print decompress(body, 15 + 32)
        else:
            print "Empty message"
    else:
        print request.body.getvalue()

    response.status = 204
    return None

run(host='localhost', port=4242, debug=False)
