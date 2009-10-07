#!/usr/bin/env python

import core
import socket, unittest

class TestHTTPConnectionQueue(unittest.TestCase):
    def setUp(self):
        address = (socket.gethostbyname('127.0.0.1'), 80)
        self.cq = core.HTTPConnectionQueue(address)

    def testQueueLength(self):
        temp = self.cq.getConnection()
        for i in range(5):
            self.assertEqual(self.cq.queue.qsize(), i)
            self.cq.putConnection(temp)

    def testResetConnection(self):
        prev = core.HTTPConnectionQueue.REQUEST_LIMIT
        try:
            core.HTTPConnectionQueue.REQUEST_LIMIT = 2
            a = self.cq.getConnection()
            self.cq.putConnection(a)
            self.assertEqual(self.cq.getConnection(), a)
            self.cq.putConnection(a)
            self.assertNotEqual(self.cq.getConnection(), a)
        finally:
            core.HTTPConnectionQueue.REQUEST_LIMIT = prev

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
        pass

class TestControlThread(unittest.TestCase):
    def setUp(self):
        pass

class TestController(unittest.TestCase):
    def setUp(self):
        pass
    
if __name__ == '__main__':
    unittest.main()
