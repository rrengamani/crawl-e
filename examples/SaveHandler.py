#!/usr/bin/env python
import gzip, sys, threading
import Crawle.core

class SaveURLHandler(Crawle.core.Handler):
    """This handler simply saves all pages. into a gziped file. Any reponses
    with status other than 200 is placed back on the queue.

    This example also demonstrates the importance of synchronization as
    multiple threads can attempt to write to the file conncurrently.
    """

    def __init__(self, output):
        self.output = gzip.open(output,'ab')
	self.lock = threading.Lock()
	self.exit = False

    def process(self, info, rmi):
        if info['status'] != 200:
            print "%d - putting %s back on queue" % (info['status'],
                                                     info['url'])
            rmi.put(info['url'])
        else:
            self.lock.acquire()
            if self.exit:
                self.lock.release()
                return
            self.output.write(info['body'])
            self.output.write("===*===\n")
            self.lock.release()
				

    def stop(self):
        self.exit = True
        self.output.close()

if __name__ == '__main__':
    Crawle.core.runCrawle(sys.argv, handler=SaveURLHandler('output.gz'))
