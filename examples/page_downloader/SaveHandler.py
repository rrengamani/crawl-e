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

    def process(self, req_res, queue):
        if not req_res.response_status:
            print req_res.error
            return

        if req_res.response_status != 200:
            print "%d - putting %s back on queue" % (req_res.response_status,
                                                     req_res.response_url)
            queue.put(req_res.response_url)
        else:
            self.lock.acquire()
            if self.exit:
                self.lock.release()
                return
            self.output.write(req_res.response_body)
            self.output.write("===*===\n")
            self.lock.release()
				

    def stop(self):
        self.exit = True
        self.output.close()

if __name__ == '__main__':
    crawle.run_crawle(sys.argv, handler=SaveURLHandler('output.gz'))
