#!/usr/bin/env python
import crawle, socket, unittest

class TestHTTPConnectionQueue(unittest.TestCase):
    def setUp(self):
        address = (socket.gethostbyname('127.0.0.1'), 80, 'http')
        self.cq = crawle.HTTPConnectionQueue(address)

    def testQueueLength(self):
        temp = self.cq.get_connection()
        for i in range(5):
            self.assertEqual(self.cq.queue.qsize(), i)
            self.cq.put_connection(temp)

    def testResetConnection(self):
        prev = crawle.HTTPConnectionQueue.REQUEST_LIMIT
        try:
            crawle.HTTPConnectionQueue.REQUEST_LIMIT = 2
            a = self.cq.get_connection()
            self.cq.put_connection(a)
            self.assertEqual(self.cq.get_connection(), a)
            self.cq.put_connection(a)
            self.assertNotEqual(self.cq.get_connection(), a)
        finally:
            crawle.HTTPConnectionQueue.REQUEST_LIMIT = prev

    def testGetConnectionCount(self):
        for i in range(5):
            conn = self.cq.get_connection()
            self.assertEqual(conn.request_count, 0)

    def testGetConnectionCountReplace(self):
        for i in range(5):
            conn = self.cq.get_connection()
            self.assertEqual(conn.request_count, i)
            self.cq.put_connection(conn)


class TestHTTPConnectionControl(unittest.TestCase):
    def setUp(self):
        self.cc = crawle.HTTPConnectionControl(crawle.Handler())

    def testRequestSTOP_CRAWLE(self):
        crawle.STOP_CRAWLE = True
        rr = crawle.RequestResponse('')
        try:
            self.cc.request(rr)
            self.fail('Did not raise CRAWL-E Stopped exception.')
        except Exception, e:
            self.assertEqual('CRAWL-E Stopped', e.args[0])
        finally:
            crawle.STOP_CRAWLE = False

    def testRequestPreProcess(self):
        rr = crawle.RequestResponse('http://google.com')
        self.cc.handler = PreProcessFailHandler()
        try:
            self.cc.request(rr)
            self.fail('Did not raise Aborted in pre_process exception.')
        except Exception, e:
            self.assertEqual('Aborted in pre_process', e.args[0])

    def testRequestInvalidMethod(self):
        rr = crawle.RequestResponse('http://www.google.com', method='INVALID')
        self.cc.request(rr)
        self.assertEqual(405, rr.response_status)

    def testRequestInvalidHostname(self):
        rr = crawle.RequestResponse('http://invalid')
        try:
            self.cc.request(rr)
            self.fail('Did not raise invalid hostname exception')
        except socket.gaierror, e:
            self.assertEqual(-5, e.errno)
        except socket.error, e:
            self.assertEqual('OPENDNS Non-existent domain', e.args[0])

    def testRequestInvalidURL(self):
        urls = ['invalid', 'http:///invalid', 'httpz://google.com']
        for url in urls:
            rr = crawle.RequestResponse(url)
            try:
                self.cc.request(rr)
                self.fail('Did not raise Invalid URL exception.')
            except Exception, e:
                self.assertEqual('Invalid URL scheme', e.args[0])

    def testRequest301(self):
        rr = crawle.RequestResponse('http://google.com', redirects=None)
        self.cc.request(rr)
        self.assertEqual(301, rr.response_status)
        self.assertEqual('http://www.google.com/',
                         rr.response_headers['location'])

    def testRequestRedirectExceeded(self):
        rr = crawle.RequestResponse('http://google.com', redirects=0)
        try:
            self.cc.request(rr)
            self.fail('Did not raise redirect count exceeeded exception.')
        except Exception, e:
            self.assertEqual('Redirect count exceeded', e.args[0])

    def testRequestSuccessfulRedirect(self):
        rr = crawle.RequestResponse('http://google.com', redirects=1)
        self.cc.request(rr)
        self.assertEqual(200, rr.response_status)
        self.assertEqual(0, rr.redirects)

    def testRequest200(self):
        rr = crawle.RequestResponse('http://www.google.com', redirects=1)
        self.cc.request(rr)
        self.assertEqual(200, rr.response_status)
        self.assertEqual(1, rr.redirects)
        self.assertTrue(rr.response_time > 0)

    def testHTTPSRequest200(self):
        # Page that can only be accessed via https, http causes redirect
        url = 'https://msp.f-secure.com/web-test/common/test.html'
        rr = crawle.RequestResponse(url, redirects=1)
        self.cc.request(rr)
        self.assertEqual(200, rr.response_status)
        self.assertEqual(1, rr.redirects)
        self.assertTrue(rr.response_time > 0)

    def testRequestGzip(self):
        rr = crawle.RequestResponse('http://www.pricetrackr.com/robots.txt',
                                    redirects=1)
        self.cc.request(rr)
        self.assertEqual(200, rr.response_status)
        self.assertEqual(1, rr.redirects)
        self.assertTrue(rr.response_time > 0)
        self.assertTrue('User-agent' in rr.response_body)

    def testRequestPost(self):
        rr = crawle.RequestResponse(
            'http://www.snee.com/xml/crud/posttest.cgi', method='POST',
            params={'fname':'CRAWL-E', 'lname':'POST_TEST'})
        self.cc.request(rr)
        self.assertEqual(200, rr.response_status)
        self.assertTrue(rr.response_time > 0)
        self.assertTrue(''.join(['<p>First name: "CRAWL-E"</p>',
                                 '<p>Last name: "POST_TEST"</p>'])
                        in rr.response_body)


###
# HELPER CLASSES
###
class PreProcessFailHandler(crawle.Handler):
    def pre_process(self, req_res): req_res.response_url = None

    
if __name__ == '__main__':
    unittest.main()
