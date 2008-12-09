import sys, threading
import Pyro.core

class PyroQueue(object, Pyro.core.ObjBase):
    """PyroQueue is an abstract class in the sense that it needs to be
    subclassed where the subclass calls Pyro.core.ObjBase.__init__(self) and
    implements the get and put method."""
	
    def get(self):
        """The get function must return three items.
        url, headers, extra
        where:
             url     - a string representing the url to crawl
             headers - a dictionary of headers to make the request with.
                       for no headers simply return None. To specifiy to the
                       crawler that there are no valid headers return False
             extra   - any additional object to pass between the queue and the
                       handler. Can be None
        """
        raise "RemoteQueueHandler.get() needs to be implemented"
    
    def put(self, queue_item):
        raise "RemoteQueueHandler.put(queue_item) needs to be implemented"


class URLQueue(PyroQueue):
    """URLQueue is the most basic queue type and is all that is needed for
    most situations. Simply, it queues full urls."""
	
    def __init__(self, seedfile=None):
        """Sets up the URLQueue by creating a queue and lock for the queue.
        
        Keyword arguments:
        seedfile -- file containing urls to seed the queue (default None)
        """
        self.queue = []
        self.lock = threading.Lock()

        # Add seeded items to the queue
        if seedfile:
            try:
                file = open(seedfile)
            except:
                raise "Could not open seed file"
            count = 0
            for line in file:
                self.queue.append(line.strip())
                count += 1
            file.close()
            print "Queued:", count
        else:
            print "Starting with empty queue"

        # Init the Pyro object
        Pyro.core.ObjBase.__init__(self)

    def save(self, file):
        """Outputs queue to file specified. On error prints queue to screen."""
        try:
            file = open(file, 'w')
        except:
            sys.stderr.write(' '.join(('Could not open file for saving.',
                                       'Printing to screen.\n')))
            sys.stderr.flush()
            file = sys.stdout

        for item in self.queue:
            file.write(item+"\n")
        if file != sys.stdout:
            file.close()

        print "Saved %d items." % len(self.queue)

    def get(self):
        """Return url at the head of the queue or None if empty"""
        self.lock.acquire()
        size = len(self.queue)
        if size is 0:
            print "Queue empty"
            url = None
        else:
            url = self.queue.pop(0)
            if size % 1000 is 0:
                print "Queue Size: %d" % size
        self.lock.release()
        return url, None, None

    def put(self, url):
        self.lock.acquire()
        self.queue.append(url)
        self.lock.release()


if __name__ == '__main__':
    """This script runs the URLQueue.
        
    It takes an optional seedfile argument which when used is also used as the
    file to dump output to on close.

    If writing a different PyroQueue subclass one must call:
        Pyro.core.initServer
        daemon.connect
        daemon.requestLoop
    See the Pyro documentation on the proper usage for these functions.
    """
	
    try:
        seedfile = sys.argv[1]
    except:
        seedfile = None

    queueHandler = URLQueue(seedfile)
    Pyro.core.initServer()
    daemon=Pyro.core.Daemon()
    uri=daemon.connect(queueHandler, "URLQueue")

    print "PYROLOC://%s:%d/URLQueue" % (daemon.hostname, daemon.port)

    try:
        daemon.requestLoop()
    except:
        queueHandler.save(seedfile)
