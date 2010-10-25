"""CRAWL-E is a highly distributed web crawling framework."""

import gzip, httplib, resource, socket, sys, threading, time, urllib, urlparse
import cStringIO, Queue

CONNECTION_TIMEOUT = 30
EMPTY_QUEUE_WAIT = 5
STOP_CRAWLE = False

class Handler(object):
    """An _abstract_ class for handling what urls to retrieve and how to
    parse and save them. The functions of this class need to be designed in
    such a way so that they are threadsafe as multiple threads will have
    access to the same instance.
    """

    def pre_process(self, request_response):
        """pre_process is called directly before making the reqeust. Any of the
        request parameters can be modified here.

        Setting the responseURL to None will cause the request to be dropped.
        This is useful for testing if a redirect link should be followed.
        """
        return
    
    def process(self, request_response, queue):
        """Process is called after the request has been made. It needs to be
        implemented by a subclass.

        Keyword Arguments:
        request_response -- the request response object
        queue -- the handler to the queue class
        """
        assert request_response and queue # pychecker hack
        raise NotImplementedError(' '.join(('Handler.process must be defined',
                                            'in a subclass')))

class RequestResponse(object):
    """This class is a container for information pertaining to requests and
    responses."""

    def __init__(self, url, headers=None, method='GET', params=None,
                 redirects=10):
        """Constructs a RequestResponse object.
        
        Keyword Arguments:
        url -- The url to request.
        headers -- The http request headers.
        method -- The http request method.
        params -- The http parameters.
        redirects -- The maximum number of redirects to follow.
        """
        self.error = None
        self.redirects = redirects

        self.request_headers = headers
        self.request_url = url
        self.request_method = method
        self.request_params = params

        self.response_status = None
        self.response_url = url
        self.response_headers = None
        self.response_body = None
        self.response_time = None


class HTTPConnectionQueue(object):
    """This class handles the queue of sockets for a particular address.

    This essentially is a queue of socket objects which also adds a transparent
    field to each connection object which is the request_count. When the
    request_count exceeds the REQUEST_LIMIT the connection is automatically
    reset.
    """
    REQUEST_LIMIT = None

    @staticmethod
    def connection_object(address, encrypted):
        """Very simply return a HTTP(S)Connection object."""
        if encrypted:
            connection = httplib.HTTPSConnection(*address)
        else:
            connection = httplib.HTTPConnection(*address)
        connection.request_count = 0
        return connection

    def __init__(self, address, encrypted=False, max_conn=None):
        """Constructs a HTTPConnectionQueue object.

        Keyword Arguments:
        address -- The address for which this object maps to.
        encrypted -- Where or not the connection is encrypted.
        max_conn -- The maximum number of connections to maintain
        """
        self.address = address
        self.encrypted = encrypted
        self.queue = Queue.Queue(0)
        self.connections = 0
        self.max_conn = max_conn

    def __del__(self):
        """Destroys the HTTPConnectionQueue object."""
        try:
            while True:
                connection = self.queue.get(block=False)
                connection.close()
        except Queue.Empty: pass

    def get(self):
        """Return a HTTP(S)Connection object for the appropriate address.
        
        First try to return the object from the queue, however if the queue
        is empty create a new socket object to return.

        Dynamically add new field to HTTPConnection called request_count to
        keep track of the number of requests made with the specific connection.
        """
        try:
            connection = self.queue.get(block=False)
            self.connections -= 1
            """Reset the connection if exceeds request limit"""
            if (self.REQUEST_LIMIT and
                connection.request_count >= self.REQUEST_LIMIT):
                connection.close()
                connection = HTTPConnectionQueue.connection_object(
                    self.address, self.encrypted)
        except Queue.Empty:
            connection = HTTPConnectionQueue.connection_object(self.address,
                                                               self.encrypted)
        return connection

    def put(self, connection):
        """Put the HTTPConnection object back on the queue."""
        connection.request_count += 1
        if self.max_conn != None and self.connections + 1 > self.max_conn:
            connection.close()
        else:
            self.queue.put(connection)
            self.connections += 1


class QueueNode(object):
    """This class handles an individual node in the CQueueLRU."""

    def __init__(self, connection_queue, key, next=None):
        """Construct a QueueNode object.

        Keyword Arguments:
        connection_queue -- The ConnectionQueue object.
        key -- The unique identifier that allows one to perform a reverse
               lookup in the hash table.
        next -- The previous least recently used item.
        """
        
        self.connection_queue = connection_queue
        self.key = key
        self.next = next
        if next:
            self.next.prev = self
        self.prev = None

    def __del__(self):
        """Properly destruct the node"""
        if self.prev:
            self.prev.next = None
        del self.connection_queue

class CQueueLRU(object):
    """This class manages a least recently used list with dictionary lookup."""

    def __init__(self, max_queues=None, max_conn=None):
        """Construct a CQueueLRU object.

        Keyword Arguments:
        max_queues -- The maximum number of unique queues to manage. When only
                      crawling a single domain, one should be sufficient.
        max_conn -- The maximum number of connections that may persist within
                    a single ConnectionQueue.
        """

        self.lock = threading.Lock()
        self.max_queues = max_queues
        self.max_conn = max_conn
        self.table = {}
        self.newest = None
        self.oldest = None

    def __getitem__(self, key):
        """Return either a HTTP(S)Connection object.

        Fetches an already utilized object if one exists.
        """
        self.lock.acquire()
        if key in self.table:
            connection = self.table[key].connection_queue.get()
        else:
            connection = HTTPConnectionQueue.connection_object(*key)
        self.lock.release()
        return connection

    def __setitem__(self, key, connection):
        """Store the HTTP(S)Connection object.

        This function ensures that there are at most max_queues. In the event
        there are too many, the oldest inactive queues will be deleted.
        """
        self.lock.acquire()
        if key in self.table:
            node = self.table[key]
            # move the node to the head of the list
            if self.newest != node:
                node.prev.next = node.next
                if self.oldest != node:
                    node.next.prev = node.prev
                else:
                    self.oldest = node.prev
                node.prev = None
                node.next = self.newest
                self.newest = node.next.prev = node
        else:
            # delete the oldest while too many
            while (self.max_queues != None and
                   len(self.table) + 1 > self.max_queues):
                if self.oldest == self.newest:
                    self.newest = None
                del self.table[self.oldest.key]
                prev = self.oldest.prev
                del self.oldest
                self.oldest = prev
            connection_queue = HTTPConnectionQueue(*key,
                                                    max_conn=self.max_conn)
            node = QueueNode(connection_queue, key, self.newest)
            self.newest = node
            if not self.oldest:
                self.oldest = node
            self.table[key] = node
        node.connection_queue.put(connection)
        self.lock.release()


class HTTPConnectionControl(object):
    """This class handles HTTPConnectionQueues by storing a queue in a
    dictionary with the address as the index to the dictionary. Additionally
    this class handles resetting the connection when it reaches a specified
    request limit.
    """
    socket.setdefaulttimeout(CONNECTION_TIMEOUT)

    def __init__(self, handler, max_queues=None, max_conn=None):
        """Constructs the HTTPConnection Control object. These objects are to
        be shared between each thread.

        Keyword Arguments:
        handler -- The Handler class for checking if a url is valid.
        max_queues -- The maximum number of connection_queues to maintain.
        max_conn -- The maximum number of connections (sockets) allowed for a
                    given connection_queue.
        """
        self.cq_lru = CQueueLRU(max_queues, max_conn)
        self.handler = handler

    def request(self, req_res):
        """Handles the request to the server."""
        if STOP_CRAWLE:
            raise Exception('CRAWL-E Stopped')

        self.handler.pre_process(req_res)
        if req_res.response_url == None:
            raise Exception('Aborted in pre_process')

        u = urlparse.urlparse(req_res.response_url)
        if u.scheme not in ['http', 'https'] or u.netloc == '':
            raise Exception('Invalid URL scheme')

        address = socket.gethostbyname(u.hostname), u.port
        encrypted = u.scheme == 'https'

        request = urlparse.urlunparse(('', '', u.path, u.params, u.query, ''))
        if req_res.request_headers:
            headers = req_res.request_headers
        else:
            headers = {}
        if 'Host' not in headers:
            headers['Host'] = u.hostname
        if 'Accept-Encoding' not in headers:
            headers['Accept-Encoding'] = 'gzip'

        connection = self.cq_lru[(address, encrypted)]
            
        try:
            start = time.time()
            if req_res.request_params:
                data = urllib.urlencode(req_res.request_params)
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
            else:
                data = ''
            connection.request(req_res.request_method, request, data, headers)
            response = connection.getresponse()
            response_time = time.time() - start
            response_body = response.read()
            self.cq_lru[(address, encrypted)] = connection
        except Exception:
            connection.close()
            raise

        if response.status in (301, 302, 303) and req_res.redirects != None:
            if req_res.redirects <= 0:
                raise Exception('Redirect count exceeded')
            req_res.redirects -= 1
            redirect_url = response.getheader('location')
            req_res.response_url = urlparse.urljoin(req_res.response_url,
                                                    redirect_url)
            self.request(req_res)
        else:
            req_res.response_time = response_time
            req_res.response_status = response.status
            req_res.response_headers = dict(response.getheaders())
            if ('content-encoding' in req_res.response_headers and
                req_res.response_headers['content-encoding'] == 'gzip'):
                temp = gzip.GzipFile(fileobj=cStringIO.StringIO(response_body))
                req_res.response_body = temp.read()
                temp.close()
            else:
                req_res.response_body = response_body


class ControlThread(threading.Thread):
    """A single thread of control"""
    EMPTY_QUEUE_RETRYS = 0
    stop_wait_event = threading.Event()

    def __init__(self, connection_control, handler, queue):
        """Sets up the ControlThread.

        Keyword Arguments:
        connection_control -- A HTTPConnectionControl object. This object is
                              shared amongst the threads
        handler -- The handler class for parsing the returned information
        queue	-- The handle to the queue class which implements get and put.
        """
        threading.Thread.__init__(self)
        self.connection_control = connection_control
        self.handler = handler
        self.queue = queue

    def run(self):
        """This is the execution order of a single thread.
        
        The threads will stop when STOP_CRAWLE becomes true, when the queue
        raises an exception, or when a returned url is None.
        """
        retry_count = 0
        global STOP_CRAWLE
        while not STOP_CRAWLE:
            try:
                request_response = self.queue.get()
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

            if request_response is None:
                ControlThread.stop_wait_event.clear()
                ControlThread.stop_wait_event.wait(EMPTY_QUEUE_WAIT)
                if ControlThread.stop_wait_event.isSet():
                    continue
                if retry_count < ControlThread.EMPTY_QUEUE_RETRYS:
                    retry_count += 1
                    continue

                if not STOP_CRAWLE:
                    sys.stdout.write("Queue empty - stopping CRAWL-E\n")
                    sys.stdout.flush()
                    STOP_CRAWLE = True
                break

            retry_count = 0
            try:
                self.connection_control.request(request_response)
            except Exception, e:
                request_response.error = e
            self.handler.process(request_response, self.queue)

            ControlThread.stop_wait_event.set()            


class Controller(object):
    """The primary controller manages all the threads."""
	
    def __init__(self, handler, queue, num_threads=1):
        """Create the controller object

        Keyword Arguments:
        handler -- The Handler class each thread will use for processing
        queue -- The handle the the queue class
        num_threads -- The number of threads to spawn (Default 1)
        """
        queues = resource.getrlimit(resource.RLIMIT_NOFILE)[0] / num_threads
        self.connection_ctrl = HTTPConnectionControl(handler=handler,
                                                     max_queues=queues,
                                                     max_conn=num_threads)
        self.handler = handler
        # HACK AROUND THIS FOR NOW
        global STOP_CRAWLE
        STOP_CRAWLE = False

        ControlThread.EMPTY_QUEUE_RETRYS = 1

        self.threads = []
        for _ in range(num_threads):
            thread = ControlThread(handler=handler, queue=queue,
                                   connection_control=self.connection_ctrl)
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
            sys.stdout.write("%d threads closed\r" % count)
            sys.stdout.flush()
        sys.stdout.write("                        \n")
        sys.stdout.flush()

    def stop(self):
        """Stops all threads gracefully"""
        global STOP_CRAWLE
        STOP_CRAWLE = True
        sys.stderr.write("Stop received\n")
        sys.stderr.flush()
        self.join()

    def crawl_finished(self):
        """Indicates the the crawl has completed."""
        return STOP_CRAWLE


class VisitURLHandler(Handler):
    """Very simple example handler which simply visits the page.
    
    This handler just demonstrates how to interact with the queue.
    """

    def process(self, info, queue):
        """Puts item back on the queue if the request was no successful."""
        if info['status'] != 200:
            print "putting %s back on queue" % info['url']
            queue.put(info['url'])


class CrawlQueue(object):
    """CrawlQueue is an abstract class in the sense that it needs to be
    subclassed with its get and put methods defined."""

    def get(self):
        """The get function must return a RequestResponse object."""
        raise NotImplementedError("CrawlQueue.get() must be implemented")
    
    def put(self, queue_item):
        """The put function should put the queue_item back on the queue."""
        assert queue_item # pychecker hack
        raise NotImplementedError("CrawlQueue.put(...) must be implemented")

class URLQueue(CrawlQueue):
    """URLQueue is the most basic queue type and is all that is needed for
    most situations. Simply, it queues full urls."""
	
    def __init__(self, seed_file=None):
        """Sets up the URLQueue by creating a queue.
        
        Keyword arguments:
        seedfile -- file containing urls to seed the queue (default None)
        """
        self.queue = Queue.Queue(0)
        self.lock = threading.Lock()
        self.start_time = self.block_time = None
        self.total_items = 0

        # Add seeded items to the queue
        if seed_file:
            try:
                fp = open(seed_file)
            except IOError:
                raise Exception("Could not open seed file")
            count = 0
            for line in fp:
                self.queue.put(line.strip())
                count += 1
            fp.close()
            print "Queued:", count
        else:
            print "Starting with empty queue"

    def save(self, save_file):
        """Outputs queue to file specified. On error prints queue to screen."""
        try:
            fp = open(save_file, 'w')
        except IOError:
            sys.stderr.write(' '.join(('Could not open file for saving.',
                                       'Printing to screen.\n')))
            sys.stderr.flush()
            fp = sys.stdout

        items = 0
        while not self.queue.empty():
            try:
                item = self.queue.get(block=False)
                fp.write("%s\n" % item)
                items += 1
            except Queue.Empty:
                print "Saving the queue is not atomic, FIXY TIME"

        if fp != sys.stdout:
            fp.close()

        print "Saved %d items." % items

    def get(self):
        """Return url at the head of the queue or None if empty"""
        try:
            url = self.queue.get(block=False)
            self.lock.acquire()
            self.total_items += 1
            if self.start_time == None:
                self.start_time = self.block_time = time.time()
            elif self.total_items % 1000 == 0:
                now = time.time()
                print 'Crawled: %d Remaining: %d RPS: %.2f (%.2f avg)' % (
                    self.total_items, self.queue.qsize(),
                    1000 / (now - self.block_time),
                    self.total_items / (now - self.start_time))
                self.block_time = now
            self.lock.release()
            return RequestResponse(url)
        except Queue.Empty:
            return None

    def put(self, url):
        """Puts the item back on the queue."""
        self.queue.put(url)


def run_crawle(argv, handler):
    """The typical way to start CRAWL-E"""
    try:
        threads = int(argv[1])
    except (IndexError, ValueError):
        sys.stderr.write("Usage: %s threads [seedfile]\n" % argv[0])
        sys.exit(1)

    try:
        seed_file = argv[2]
    except IndexError:
        seed_file = None

    queue_handler = URLQueue(seed_file)

    controller = Controller(handler=handler, queue=queue_handler,
                            num_threads=threads)
    controller.start()
    try:
        controller.join()
    except KeyboardInterrupt:
        controller.stop()
    queue_handler.save(seed_file)

if __name__ == '__main__':
    """Basic example of how to start CRAWL-E."""
    run_crawle(sys.argv, handler=VisitURLHandler())
