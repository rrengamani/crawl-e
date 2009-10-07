#!/usr/bin/env python

import core
import socket, unittest

class TestHTTPConnectionQueue(unittest.TestCase):
    def setUp(self):
        self.address = (socket.gethostbyname('127.0.0.1'), 80)

    def testQueueLength(self):
        connection_queue = core.HTTPConnectionQueue(self.address)
        temp = connection_queue.getConnection()
        for i in range(5):
            self.assertEqual(connection_queue.queue.qsize(), i)
            connection_queue.putConnection(temp)

    def testResetConnection(self):
        prev = core.HTTPConnectionQueue.REQUEST_LIMIT = 2
        connection_queue = core.HTTPConnectionQueue(self.address)
        a = connection_queue.getConnection()
        connection_queue.putConnection(a)
        b = connection_queue.getConnection()
        self.assertEqual(a, b)
        connection_queue.putConnection(a)
        b = connection_queue.getConnection()
        self.assertNotEqual(a, b)
        core.HTTPConnectionQueue.REQUEST_LIMIT = prev

    def testGetConnectionCount(self):
        connection_queue = core.HTTPConnectionQueue(self.address)
        for i in range(5):
            conn = connection_queue.getConnection()
            self.assertEqual(conn.requestCount, 0)

    def testGetConnectionCountReplace(self):
        connection_queue = core.HTTPConnectionQueue(self.address)
        for i in range(5):
            conn = connection_queue.getConnection()
            self.assertEqual(conn.requestCount, i)
            connection_queue.putConnection(conn)

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
