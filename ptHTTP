__author__ = 'jmoffitt'

import requests
import threading #needed for any streaming HTTP connection
import json

#TODO: header details (differences in GET/POST/DELETE/STREAM)


'''
   A generic class for POSTing requests to Gnip APIs.
   These include adding/deleting Rules, submitting Historical jobs, etc.
'''
class ptHTTP():
    def __init__(self, url = None, auth = None, headers = None):
        if url is not None:
            self.url = url
        if auth is not None:
            self.auth = auth
        if headers is not None:
            self.headers = headers
        else:
            self.headers = {'content-type': 'application/json'}  #Reasonable default for PowerTrack?

    def setAuth(self, auth):
        self.auth = auth

    def setData(self, data):
        self.data = data

    def setURL(self, url):
        self.url = url

    def addHeader(self, header):
        #Grab header string and split with ":"
        headerParts = header.split(":")
        #self.headers is a dictionary, so now add this new header to dictionary
        self.headers[headerParts[0]]  = headerParts[1]

    def POST(self, data = None):
        if data is not None:
            self.data = data
        response = requests.post(self.url, auth=self.auth, data=self.data, headers=self.headers)
        return response

    def GET(self):
        response = requests.get(self.url, auth=self.auth, headers=self.headers)
        return response

    def DELETE(self, data = None):
        if data is not None:
            self.data = data
        response = requests.delete(self.url, auth=self.auth, data = self.data, headers=self.headers)
        return response


'''
   These Stream and Worker class were based on 'gnippy' classes provided at:
   gnippy - GNIP for Python

   Available here:
   http://pypi.python.org/pypi/gnippy/0.1.2

__title__ = 'gnippy'
__version__ = '0.1.2'
__author__ = 'Abhinav Ajgaonkar'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2012 Abhinav Ajgaonkar'
'''
class ptStream():
    """
         ptStream allows you to connect to the GNIP
         power track stream and fetch data
     """
    def __init__(self, url, auth, callback):
        self.url = url
        self.auth = auth
        self.callback = callback

    def connect(self):
        self.worker = ptWorker(self.url, self.auth, self.callback)
        self.worker.setDaemon(True)
        self.worker.start()

    def disconnect(self):
        self.worker.stop()
        self.worker.join()


class ptWorker(threading.Thread):
    """ Background worker to fetch data without blocking """
    def __init__(self, url, auth, callback):
        super(ptWorker, self).__init__()
        self.url = url
        self.auth = auth
        self.on_data = callback
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        r = requests.get(self.url, auth=self.auth, prefetch=False)
        for line in r.iter_lines():
            if self.stopped():
                break
            elif line:
                self.on_data(line)
