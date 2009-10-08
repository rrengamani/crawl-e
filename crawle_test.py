#!/usr/bin/env python

import crawle
import socket, unittest

class TestHTTPConnectionQueue(unittest.TestCase):
    def setUp(self):
        address = (socket.gethostbyname('127.0.0.1'), 80)
        self.cq = crawle.HTTPConnectionQueue(address)

    def testQueueLength(self):
        temp = self.cq.getConnection()
        for i in range(5):
            self.assertEqual(self.cq.queue.qsize(), i)
            self.cq.putConnection(temp)

    def testResetConnection(self):
        prev = crawle.HTTPConnectionQueue.REQUEST_LIMIT
        try:
            crawle.HTTPConnectionQueue.REQUEST_LIMIT = 2
            a = self.cq.getConnection()
            self.cq.putConnection(a)
            self.assertEqual(self.cq.getConnection(), a)
            self.cq.putConnection(a)
            self.assertNotEqual(self.cq.getConnection(), a)
        finally:
            crawle.HTTPConnectionQueue.REQUEST_LIMIT = prev

    def testGetConnectionCount(self):
        for i in range(5):
            conn = self.cq.getConnection()
            self.assertEqual(conn.requestCount, 0)

    def testGetConnectionCountReplace(self):
        for i in range(5):
            conn = self.cq.getConnection()
            self.assertEqual(conn.requestCount, i)
            self.cq.putConnection(conn)

class TestHTTPConnectionControl(unittest.TestCase):
    def setUp(self):
        self.cc = crawle.HTTPConnectionControl(crawle.Handler)
        self.cc.handler = HackedHandler

    def testRequestSTOP_CRAWLE(self):
        crawle.STOP_CRAWLE = True
        rr = crawle.RequestResponse('')
        self.cc.request(rr)
        self.assertEqual('CRAWL-E Stopped', rr.errorMsg)
        crawle.STOP_CRAWLE = False

    def testRequestPreProcess(self):
        rr = crawle.RequestResponse('http://google.com')
        self.cc.handler = preProcessFailHandler
        self.cc.request(rr)
        self.assertEqual('Aborted in preProcess', rr.errorMsg)

    def testRequestInvalidMethod(self):
        rr = crawle.RequestResponse('http://www.google.com', method='INVALID')
        self.cc.request(rr)
        self.assertEqual(400, rr.responseStatus)

    def testRequestInvalidHostname(self):
        rr = crawle.RequestResponse('http://invalid')
        self.cc.request(rr)
        self.assertEqual('Socket Error', rr.errorMsg)
        self.assertEqual('No address associated with hostname',
                         rr.errorObject.args[1])

    def testRequestInvalidURL(self):
        urls = ['invalid', 'http:///invalid', 'https://google.com']
        for url in urls:        
            rr = crawle.RequestResponse(url)
            self.cc.request(rr)
            self.assertEqual('Invalid URL', rr.errorMsg)

    def testRequest301(self):
        rr = crawle.RequestResponse('http://google.com', redirects=None)
        self.cc.request(rr)
        self.assertEqual(301, rr.responseStatus)
        self.assertEqual('http://www.google.com/',
                         rr.responseHeaders['location'])

    def testRequestRedirectExceeded(self):
        rr = crawle.RequestResponse('http://google.com', redirects=0)
        self.cc.request(rr)
        self.assertEqual('Redirect count exceeded', rr.errorMsg)

    def testRequestSuccessfulRedirect(self):
        rr = crawle.RequestResponse('http://google.com', redirects=1)
        self.cc.request(rr)
        self.assertEqual(200, rr.responseStatus)
        self.assertEqual(0, rr.redirects)

    def testRequest200(self):
        rr = crawle.RequestResponse('http://www.google.com', redirects=1)
        self.cc.request(rr)
        self.assertEqual(200, rr.responseStatus)
        self.assertEqual(1, rr.redirects)
        self.assertTrue(rr.responseTime > 0)

    def testRequestPost(self):
        rr = crawle.RequestResponse(
            'http://www.snee.com/xml/crud/posttest.cgi', method='POST',
            params={'fname':'CRAWL-E', 'lname':'POST_TEST'})
        self.cc.request(rr)
        self.assertEqual(200, rr.responseStatus)
        self.assertTrue(rr.responseTime > 0)
        self.assertTrue(''.join(['<p>First name: "CRAWL-E"</p>',
                                 '<p>Last name: "POST_TEST"</p>'])
                        in rr.responseBody)

###
# HELPER CLASSES
###

class HackedHandler(crawle.Handler):
    @staticmethod
    def preProcess(reqRes): pass

class preProcessFailHandler(crawle.Handler):
    @staticmethod
    def preProcess(reqRes): reqRes.responseURL = None

    
if __name__ == '__main__':
    unittest.main()
