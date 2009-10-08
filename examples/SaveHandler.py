#!/usr/bin/env python
import gzip, sys, threading
import crawle

class SaveURLHandler(crawle.Handler):
    """This handler simply saves all pages. into a gziped file. Any reponses
    with status other than 200 is placed back on the queue.

    This example also demonstrates the importance of synchronization as
    multiple threads can attempt to write to the file conncurrently.
    """

    def __init__(self, output):
        self.output = gzip.open(output,'ab')
	self.lock = threading.Lock()
	self.exit = False

    def process(self, reqRes, queue):
        if reqRes.responseStatus != 200:
            print "%d - putting %s back on queue" % (reqRes.responseStatus,
                                                     reqRes.responseURL)
            queue.put(reqRes.responseURL)
        else:
            self.lock.acquire()
            if self.exit:
                self.lock.release()
                return
            self.output.write(reqRes.responseBody)
            self.output.write("===*===\n")
            self.lock.release()
				

    def stop(self):
        self.exit = True
        self.output.close()

if __name__ == '__main__':
    crawle.runCrawle(sys.argv, handler=SaveURLHandler('output.gz'))
