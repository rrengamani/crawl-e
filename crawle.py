import cStringIO, gzip, httplib, socket, sys, threading, time, urllib, urlparse
import Queue

CONNECTION_TIMEOUT = 30
EMPTY_QUEUE_WAIT = 5
STOP_CRAWLE = False

class Handler(object):
    """An _abstract_ class for handling what urls to retrieve and how to
    parse and save them. The functions of this class need to be designed in
    such a way so that they are threadsafe as multiple threads will have
    access to the same instance.
    """

    def preProcess(self, requestResponse):
        """PreProcess is called directly before making the reqeust. Any of the
        request parameters can be modified here.

        Setting the responseURL to None will cause the request to be dropped.
        This is useful for testing if a redirect link should be followed.
        """
        return
    
    def process(self, requestResponse, queue):
        """Process is called after the request has been made. It needs to be
        implemented by a subclass.

        Keyword Arguments:
        requestResponse -- the request response object
        queue -- the handler to the queue class
        """
        raise NotImplementedError(' '.join(('Handler.process must be defined',
                                            'in a subclass')))

class RequestResponse(object):
    """This class is a container for information pertaining to requests and
    responses.

    Attributes:
    	redirects	- None if request should not redirect, otherwise a
			  number > 0 to indicate how many redirects to support.
    """

    def __init__(self, url, headers=None, method='GET', params=None,
                 redirects=10):
        self.requestHeaders = headers
        self.requestURL = url
        self.requestMethod = method
        self.requestParams = params
        self.redirects = redirects

        self.responseStatus = None
        self.responseURL = url
        self.responseHeaders = None
        self.responseBody = None
        self.responseTime = None

        self.errorMsg = None
        self.errorObject = None


class HTTPConnectionQueue(object):
    """This class handles the queue of sockets for a particular address.

    This essentially is a queue of socket objects which also adds a transparent
    field to each connection object which is the requestCount. When the
    requestCount exceeds the REQUEST_LIMIT the connection is automatically
    reset.
    """
    REQUEST_LIMIT = None

    def __init__(self, address):
        """Constructs a HTTPConnectionQueueobject.

        Keyword Arguments:
        address -- The address for which this object maps to.
        """
        self.address = address
        self.queue = Queue.Queue(0)

    def getConnection(self):
        """Return a HTTP(S)Connection object for the appropriate address.
        
        First try to return the object from the queue, however if the queue
        is empty create a new socket object to return.

        Dynamically add new field to HTTPConnection called requestCount to
        keep track of the number of requests made with the specific connection.
        """
        host, port, scheme = self.address
        try:
            connection = self.queue.get(block=False)
            """Reset the connection if exceeds request limit"""
            if self.REQUEST_LIMIT and \
                    connection.requestCount >= self.REQUEST_LIMIT:
                connection.close()
                if scheme != 'https':
                    connection = httplib.HTTPConnection(host, port)
                else:
                    connection = httplib.HTTPSConnection(host, port)
                connection.requestCount = 0
        except Queue.Empty:
            if scheme != 'https':
                connection = httplib.HTTPConnection(host, port)
            else:
                connection = httplib.HTTPSConnection(host, port)
            connection.requestCount = 0
        
        return connection

    def putConnection(self, connection):
        """Put the HTTPConnection object back on the queue."""
        connection.requestCount += 1
        self.queue.put(connection)


class HTTPConnectionControl(object):
    """This class handles HTTPConnectionQueues by storing a queue in a
    dictionary with the address as the index to the dictionary. Additionally
    this class handles resetting the connection when it reaches a specified
    request limit."""
    
    socket.setdefaulttimeout(CONNECTION_TIMEOUT)

    def __init__(self, handler):
        """Constructs the HTTPConnection Control object. These objects are to
        be shared between each thread.

        Keyword Arguments:
        handler -- The Handler class for checking if a url is valid.
        """
        
        self.connectionQueues = {}
        self.lock = threading.Lock()
        self.handler = handler

    def request(self, reqRes):
        """Handles the request to the server.
        """
        if STOP_CRAWLE:
            reqRes.errorMsg = 'CRAWL-E Stopped'
            return

        self.handler.preProcess(reqRes)
        if reqRes.responseURL == None:
            reqRes.errorMsg = 'Aborted in preProcess'
            return

        u = urlparse.urlparse(reqRes.responseURL)
        if u.scheme not in ['http', 'https'] or u.netloc == '':
            reqRes.errorMsg = 'Invalid URL'
            return

        try:
            address = (socket.gethostbyname(u.hostname), u.port, u.scheme)
        except socket.error, e:
            reqRes.errorMsg = 'Socket Error'
            reqRes.errorObject = e
            return

        request = urlparse.urlunparse(('', '', u.path, u.params, u.query, ''))
        if reqRes.requestHeaders:
            headers = reqRes.requestHeaders
        else:
            headers = {}
        if 'Host' not in headers:
            headers['Host'] = u.hostname
        if 'Accept-Encoding' not in headers:
            headers['Accept-Encoding'] = 'gzip'

        self.lock.acquire()
        try:
            connectionQueue = self.connectionQueues[address]
        except:
            connectionQueue = HTTPConnectionQueue(address)
            self.connectionQueues[address] = connectionQueue
        self.lock.release()
        connection = connectionQueue.getConnection()
            
        try:
            start = time.time()
            if reqRes.requestParams:
                data = urllib.urlencode(reqRes.requestParams)
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
            else:
                data = ''
            connection.request(reqRes.requestMethod, request, data, headers)
            response = connection.getresponse()
            responseTime = time.time() - start
            responseBody = response.read()
            connectionQueue.putConnection(connection)
        except httplib.ResponseNotReady:
            sys.stderr.write(' '.join(('A previous request did not call'
                                       'read(). This shouldn\'t happen\n')))
            sys.stderr.flush()
            connection.close()
            reqRes.errorMsg = 'Response not ready'
            return
        except httplib.BadStatusLine:
            connection.close()
            reqRes.errorMsg = 'Bad status line'
            return
        except socket.error, e:
            connection.close()
            reqRes.errorMsg = 'Socket error'
            reqRes.errorObject = e
            return
        except Exception, e:
            sys.stderr.write('Unhandled exception -- FIXY TIME\n')
            sys.stderr.write("%s: %s\n" % (str(type(e)), e.__str__()))
            sys.stderr.flush()
            connection.close()
            reqRes.errorMsg = 'Unhandled exception'
            reqRes.errorObject = e
            return

        # Handle redirect
        if response.status in (301, 302, 303) and reqRes.redirects != None:
            if reqRes.redirects <= 0:
                reqRes.errorMsg = 'Redirect count exceeded'
                return
            reqRes.redirects -= 1
            redirectURL = response.getheader('location')
            reqRes.responseURL = urlparse.urljoin(reqRes.responseURL,
                                                  redirectURL)
            self.request(reqRes)
            return

        reqRes.responseTime = responseTime
        reqRes.responseStatus = response.status
        reqRes.responseHeaders = dict(response.getheaders())
        if 'content-encoding' in reqRes.responseHeaders and \
                reqRes.responseHeaders['content-encoding'] == 'gzip':
            temp = gzip.GzipFile(fileobj=cStringIO.StringIO(responseBody))
            reqRes.responseBody = temp.read()
            temp.close()
        else:
            reqRes.responseBody = responseBody


class ControlThread(threading.Thread):
    """A single thread of control"""

    EMPTY_QUEUE_RETRYS = 0
    stopWaitEvent = threading.Event()

    def __init__(self, connectionControl, handler, queue):
        """Sets up the ControlThread.

        Keyword Arguments:
        connectionControl -- A HTTPConnectionControl object. This object is
                             shared amongst the threads
        handler -- The handler class for parsing the returned information
        queue	-- The handle to the queue class which implements get and put.
        """
        threading.Thread.__init__(self)
        self.connectionControl = connectionControl
        self.handler = handler;
        self.queue = queue

    def run(self):
        """This is the execution order of a single thread.
        
        The threads will stop when STOP_CRAWLE becomes true, when the queue
        raises an exception, or when a returned url is None.
        """

        retryCount = 0
        global STOP_CRAWLE
        while not STOP_CRAWLE:
            try:
                requestResponse = self.queue.get()
            except Exception, e:
                if not STOP_CRAWLE:
                    sys.stderr.write("Queue error - stopping CRAWL-E\n")
                    sys.stderr.flush()
                    STOP_CRAWLE = True
                sys.stderr.write("%s: %s\n" % (str(type(e)), e.__str__()))
                break

        # The thread notification needs to change a bit to take account of
        # threads which may be working at the time the queue is empty, rather
        # than simply sleeping for a given time period.

            if requestResponse is None:
                ControlThread.stopWaitEvent.clear()
                ControlThread.stopWaitEvent.wait(EMPTY_QUEUE_WAIT)
                if ControlThread.stopWaitEvent.isSet():
                    continue
                if retryCount < ControlThread.EMPTY_QUEUE_RETRYS:
                    retryCount += 1
                    continue

                if not STOP_CRAWLE:
                    sys.stderr.write("Queue empty - stopping CRAWL-E\n")
                    sys.stderr.flush()
                    STOP_CRAWLE = True
                break

            retryCount = 0
            self.connectionControl.request(requestResponse)
            self.handler.process(requestResponse, self.queue)

            # Now release waiting threads
            ControlThread.stopWaitEvent.set()            


class Controller(object):
    """The primary controller manages all the threads."""
	
    def __init__(self, handler, queue, numThreads=1):
        """Create the controller object

        Keyword Arguments:
        handler -- The Handler class each thread will use for processing
        queue -- The handle the the queue class
        numThreads -- The number of threads to spwan (Default 1)
        """
        self.threads = []
        self.connectionCtrl = HTTPConnectionControl(handler=handler)
        self.handler = handler
        # HACK AROUND THIS FOR NOW
        global STOP_CRAWLE
        STOP_CRAWLE = False

        ControlThread.EMPTY_QUEUE_RETRYS = 1

        for x in range(numThreads):
            thread = ControlThread(handler=handler, queue=queue,
                                   connectionControl=self.connectionCtrl)
            self.threads.append(thread)

    def start(self):
        """Starts all threads"""
        for thread in self.threads:
            thread.start()

    def join(self):
        """Join on all threads"""
        count = 0
        for thread in self.threads:
            while 1:
                thread.join(1)
                if not thread.isAlive():
                    break
            count += 1
            sys.stderr.write("%d threads closed\r" % count)
            sys.stderr.flush()
        sys.stderr.write("                        \n")
        sys.stderr.flush()

    def stop(self):
        """Stops all threads gracefully"""
        global STOP_CRAWLE
        STOP_CRAWLE = True
        sys.stderr.write("Stop received\n")
        sys.stderr.flush()
        self.join()

    def crawl_finished(self):
        return STOP_CRAWLE


class VisitURLHandler(Handler):
    """Very simple example handler which simply visits the page.
    
    This handler just demonstrates how to interact with the queue.
    """

    def process(self, info, queue):
        if info['status'] != 200:
            print "putting %s back on queue" % info['url']
            queue.put(info['url'])


class CrawlQueue(object):
    """CrawlQueue is an abstract class in the sense that it needs to be
    subclassed with its get and put methods defined."""

    def get(self):
        """The get function must return a RequestResponse object."""
        raise "CrawlQueue.get() needs to be implemented"
    
    def put(self, queueItem):
        raise "CrawlQueue.put(queue_item) needs to be implemented"

class URLQueue(CrawlQueue):
    """URLQueue is the most basic queue type and is all that is needed for
    most situations. Simply, it queues full urls."""
	
    def __init__(self, seedfile=None):
        """Sets up the URLQueue by creating a queue.
        
        Keyword arguments:
        seedfile -- file containing urls to seed the queue (default None)
        """
        self.queue = Queue.Queue(0)
        self.lock = threading.Lock()
        self.startTime = self.blockTime = None
        self.totalItems = 0

        # Add seeded items to the queue
        if seedfile:
            try:
                file = open(seedfile)
            except:
                raise "Could not open seed file"
            count = 0
            for line in file:
                self.queue.put(line.strip())
                count += 1
            file.close()
            print "Queued:", count
        else:
            print "Starting with empty queue"

    def save(self, file):
        """Outputs queue to file specified. On error prints queue to screen."""
        try:
            file = open(file, 'w')
        except:
            sys.stderr.write(' '.join(('Could not open file for saving.',
                                       'Printing to screen.\n')))
            sys.stderr.flush()
            file = sys.stdout

        items = 0
        while not self.queue.empty():
            try:
                item = self.queue.get(block=False)
                file.write("%s\n" % item)
                items += 1
            except Queue.Empty:
                print "Saving the queue is not atomic, FIXY TIME"

        if file != sys.stdout:
            file.close()

        print "Saved %d items." % items

    def get(self):
        """Return url at the head of the queue or None if empty"""
        try:
            url = self.queue.get(block=False)
            self.lock.acquire()
            self.totalItems += 1
            if self.startTime == None:
                self.startTime = self.blockTime = time.time()
            elif self.totalItems % 1000 == 0:
                now = time.time()
                print 'Crawled: %d Remaining: %d RPS: %.2f (%.2f avg)' % (
                    self.totalItems, self.queue.qsize(),
                    1000 / (now - self.blockTime),
                    self.totalItems / (now - self.startTime))
                self.blockTime = now
            self.lock.release()
            return RequestResponse(url)
        except Queue.Empty:
            return None

    def put(self, url):
        self.queue.put(url)


def runCrawle(argv, handler):
    """The typical way to start CRAWL-E"""
    try:
        threads = int(argv[1])
    except:
        sys.stderr.write("Usage: %s threads [seedfile]\n" % argv[0])
        sys.exit(1)

    try:
        seedfile = argv[2]
    except:
        seedfile = None

    queueHandler = URLQueue(seedfile)

    controller = Controller(handler=handler, queue=queueHandler,
                            numThreads=threads)
    controller.start()
    try:
        controller.join()
    except KeyboardInterrupt:
        controller.stop()
    queueHandler.save(seedfile)


if __name__ == '__main__':
    """Basic example of how to start CRAWL-E."""
    runCrawle(sys.argv, handler=VisitURLHandler())
