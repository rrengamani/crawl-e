import httplib, socket, sys, threading, urlparse
import Pyro.core

CONNECTION_TIMEOUT = 30
MAX_DEPTH = 10
STOP_CRAWLE = False

def runCrawle(argv, handler, auth=None):
    """The typical to start CRAWL-E"""
    try:
        rmi_url = argv[1].strip()
        threads = int(argv[2])
    except:
        sys.stderr.write("Usage: %s rmi_url threads\n" % argv[0])
        sys.exit(1)

    controller = Controller(handler=handler, auth=auth, RMI_URL=rmi_url,
                            numThreads=threads)
    controller.start()

    try:
        sys.stderr.write("---ctrl+c to quit---\n")
        sys.stderr.flush()
        while sys.stdin.readline():
            pass
    except KeyboardInterrupt:
        pass

    controller.stop()


class Auth(object):
    """An _abstract_ class for handling authentication.
    Python doesn't have real abstract classes, but its cool

    The variable terminate is checked periodically to see if the threads
    which hold this class should shutdown.
    """

    terminate = False
    
    def getNextHeader(self, url):
        """Return the next header to use in the HTTP request.

	Additionally this function can be used to verify that the user is
	still logged in through the URL.
	"""
	raise NotImplementedError(' '.join(('Auth.getNextHeader must be',
					    'defined in a subclass')))

    def isValidBody(self, body):
        """Function called by to verify the body of the request is as expected.

	By default this function always returns True unless it is extended in
        a subclass.
	"""
	return True
    
    def stop(self):
        """Called when getNextURL returns none.

	This is useful to avoid sleeping and to allow threads to close
	cleanly. This function need not be extended by a subclass, however
	subclasses should check the value of terminate in getNextHeader.
	"""
	self.terminate = True

    def save(self):
        """This function should be extended in a subclass to handle
        termination.
        """
	sys.stderr.write("Auth.save() is not implemented\n")
        sys.stderr.flush()


class Handler(object):
    """An _abstract_ class for handling what urls to retrieve and how to
    parse and save them. The functions of this class need to be designed in
    such a way so that they are threadsafe as multiple threads will have
    access to the same instance.
    """
    
    def process(self, info, rmi):
        """Process is called after the request has been made. It needs to be
        implemented by a subclass.

        Keyword Arguments:
        info -- a dictionary containing:
                    status -- the returned HTTP status
                    body -- the page content
                    url -- the initial requested url
                    final_url -- the final url after redirects
                    headers -- the HTTP response headers
        rmi -- the PythonRMI object.
        """
        raise NotImplementedError(' '.join(('Handler.process must be defined',
                                            'in a subclass')))

    def isValidURL(self, url):
        """This function is called before processing a redirect. Implementing
        this function in a subclass can greatly improve performace.
        """
        return True

class HTTPConnectionQueue(object):
    """This class handles the queue of sockets for a particular address.
    
    Expand on this.
    """

    def __init__(self, address):
        """Constructs a HTTPConnectionQueue object.

        Keyword Arguments:
        address -- The address for which this object maps to.
        """
        self.address = address
        self.lock = threading.Lock()
        self.queue = []
        self.size = 0

    def closeConnections(self):
        self.lock.acquire()
        sys.stderr.write("Closing %d connections\n" % len(self.queue))
        sys.stderr.flush()
        for connection in self.queue:
            connection.close()
        self.lock.release()

    def getConnection(self):
        """Return a HTTPConnection object for the appropriate address.
        
        First try to return the object from the queue, however if the queue
        is empty create a new socket object to return.

        Dynamically add new field to HTTPConnection called requestCount to
        keep track of the number of requests made with the specific connection.
        """            
        self.lock.acquire()
        if self.size is 0:
            self.lock.release()
            new = httplib.HTTPConnection(self.address)
            new.requestCount = 1
            return new
        connection = self.queue.pop(0)
        self.size -= 1
        self.lock.release()
        return connection

    def putSocket(self, connection):
        if STOP_CRAWLE:
            connection.close()
            return
        """Put the HTTPConenction object back on the queue."""            
        self.lock.acquire()
        self.size += 1
        connection.requestCount += 1
        self.queue.append(connection)
        self.lock.release()


class HTTPConnectionControl(object):
    """This class handles HTTPConnectionQueues by storing a queue in a
    dictionary with the address as the index to the dictionary. Additionally
    this class handles resetting the connection when it reaches a specified
    request limit."""
    
    socket.setdefaulttimeout(CONNECTION_TIMEOUT)

    def __init__(self, handler, auth=None, requestLimit=-1, doRedirect=True):
        """Constructs the HTTPConnection Control object. These objects are to
        be shared between each thread.

        Keyword Arguments:
        handler -- The Handler class for checking if a url is valid.
        auth -- The Auth class for getting the next header (Default None).
        requestLimit -- The maximum number of requests a HTTP session can make.
                        When reached, the connection is closed and a new
                        connection is established in its place (Default -1).
        doRedirect -- If true, the redirected URL will automatically be placed
                      back on the queue. Otherwise the Handler process
                      function will have to handle a return with a HTTP
                      redirect status.
        """
        
        self.connectionQueues = {}
        self.lock = threading.Lock()
        self.handler = handler
        self.auth = auth
        self.requestLimit = requestLimit
        self.doRedirect = doRedirect

    def __resetConnection(self, connection, address):
        """Restart the connection."""
        connection.close()
        connection = httplib.HTTPConnection(address)
        connection.requestCount = 1

    def stop(self):
        """Used to indicate to stop processing requests"""
        [q.closeConnections() for q in self.connectionQueues.itervalues()]

    def request(self, url, depth):
        """Handles the request to the server.

        On success, return a dictionary containing:
        status, headers, body, url, final_url
        as described in handler	process.

        On failure the status values are negative and mean the following:
            -1.0 -- Response not ready
            -1.1 -- Bad status line
            -1.2 -- Socket error
            -1.3 -- Unhandled exception
            -2   -- Stop has been called
            -3   -- Redirect depth has been exceeded
            -4   -- Unsupported protocol (only HTTP currently supported)
            -5   -- Auth class returned None
            -6   -- gethostbyname failed
        """
        if STOP_CRAWLE:
            return {'status':-2, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}
        if depth > MAX_DEPTH:
            return {'status':-3, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}

        protocol, domain, request, _, parameters, _ = urlparse.urlparse(url)
        if parameters != '':
            request = '?'.join((request, parameters))

        try:
            address = socket.gethostbyname(domain)
        except socket.error:
            return {'status':-6, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}

        self.lock.acquire()
        try:
            connectionQueue = self.connectionQueues[address]
        except:
            connectionQueue = HTTPConnectionQueue(address)
            self.connectionQueues[address] = connectionQueue
        self.lock.release()
        connection = connectionQueue.getConnection()
            
        if connection.requestCount is self.requestLimit:
            self.__resetConnection(connection,address)

        if self.auth:
            headers = self.auth.getNextHeader(url)
            if headers is None:
                return {'status':-5, 'headers':'', 'body':'', 'url':url,
                        'final_url':url}
        else:
            headers = {}
        headers['Host'] = domain

        # This should be deleted at somepoint when https is handled
        # It needs to be here because it needs to be checked after
        # auth.getNextHeader in cases where reauthentication is
        # required.
        if protocol != 'http':
            return {'status':-4, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}
        # This needs to be checked here after we checked
        # our auth header, otherwise we might not know
        # when to log back in, or put the url back on the
        # queue
        if not self.handler.isValidURL(url):
            return None

        try:
            connection.request('GET', request, '', headers)
            response = connection.getresponse()
            body = response.read()
            connectionQueue.putSocket(connection)
        except httplib.ResponseNotReady:
            sys.stderr.write(' '.join(('A previous request did not call'
                                       'read(). This shouldn\'t happen\n')))
            sys.stderr.flush()
            connection.close()
            return {'status':-1.0, 'headers':'', 'body':'', 'url':url, 
                    'final_url':url}
        except httplib.BadStatusLine:
            connection.close()
            return {'status':-1.1, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}
        except socket.error:
            connection.close()
            return {'status':-1.2, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}
        except:
            sys.stderr.write('Unhandled exception -- FIXY TIME\n')
            sys.stderr.flush()
            connection.close()
            return {'status':-1.3, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}

        # Handle redirecting by first verifying the URL
        # and then making the request. Return the response.
        if self.doRedirect and response.status in (301,302):
            url1 = urlparse.urljoin(url, response.getheader('Location'))
            retryReturn = self.request(url1, depth + 1)
            if retryReturn == None:
                return None
            retryReturn['url'] = url
            return retryReturn

        if self.auth and not self.auth.isValidBody(body):
            return {'status':-5, 'headers':'', 'body':'', 'url':url,
                    'final_url':url}

        toReturn = {}
        toReturn['status'] = response.status
        try: # This only works in Python 2.5
            toReturn['headers'] = response.getheaders()
        except:
            toReturn['headers'] = None
        toReturn['body'] = body
        toReturn['url'] = url
        toReturn['final_url'] = url
        return toReturn

class ControlThread(threading.Thread):
    """A single thread of control"""

    def __init__(self, connectionControl, handler, RMI_URL, auth=None):
        """Sets up the ControlThread.

        Keyword Arguments:
        connectionControl -- A HTTPConnectionControl object. This object is
                             shared amongst the threads
        handler -- The handler class for parsing the returned information
        RMI_URL -- The RMI initilization URL. Each thread needs to connect
                   separately to the RMI server.
        auth -- The optional auth class which can provide cookies, deal with
                rate limiting, etc.
        """
        threading.Thread.__init__(self)
        self.connectionControl = connectionControl
        self.handler = handler;
        self.auth = auth
        self.rmi = Pyro.core.getProxyForURI(RMI_URL)

    def run(self):
        """This is the execution order of a single thread.
        
        The threads will stop when STOP_CRAWLE becomes true, when the RMI
        server is shutdown, and when a returned url is None.
        """

        global STOP_CRAWLE
        while not STOP_CRAWLE:
            # Get the URL to retrieve
            try:
                url = self.rmi.get()
            except:
                sys.stderr.write("RMI.get failed. Thread shutting down\n")
                sys.stderr.flush()
                STOP_CRAWLE = True
                break
			
            if url is None:
                if self.auth:
                    self.auth.stop()
                    STOP_CRAWLE = True
                    break

            response = self.connectionControl.request(url, 0)
            if response:
                self.handler.process(response, self.rmi)

class Controller(object):
    """The primary controller manages all the threads."""
	
    def __init__(self, handler, RMI_URL, auth=None, numThreads=1,
                 requestLimit=-1, doRedirect=True):
        """Create the controller object

        Keyword Arguments:
        handler -- The Handler class each thread will use for processing
        RMI_URL -- The url to the RMI server
        auth -- The optional Auth class (Default None)
        numThreads -- The number of threads to spwan (Default 1)
        requestLimit -- See HTTPConnectionControl
        doRedirect -- If true redirects will transparently be handled
        """
        self.threads = []
        self.connection_ctrl = HTTPConnectionControl(auth=auth,
                                                     handler=handler,
                                                     requestLimit=requestLimit,
                                                     doRedirect=doRedirect)
        self.auth = auth
        self.handler = handler

        for x in range(numThreads):
            thread = ControlThread(handler=handler,
                                   RMI_URL=RMI_URL, auth=auth,
                                   connectionControl = self.connection_ctrl)
            self.threads.append(thread)

    def start(self):
        """Starts all threads"""
        for thread in self.threads:
            thread.start()

    def join(self):
        """Join on all threads"""
        count = 0
        for thread in self.threads:
            thread.join()
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
        self.connection_ctrl.stop()
        self.join()
        if self.auth:
            self.auth.save()

class BrowserAuth(Auth):
    """A simple Auth class which sets the HTTP USER-AGENT"""
	
    _USER_AGENT = ' '.join(('Mozilla/5.0 (Macintosh; U; PPC Mac OS X Mach-0;',
                            'en-US; rv:1.8.1.7) Gecko/20070914',
                            'Firefox/2.0.0.7'))
	
    def __init__(self, user_agent=None):
        """Constructs the BrowserAuth class

        Keyword Arguments:
        user_agent -- The desired user_agent string to use.
        """
		
        if user_agent is None:
            self.headers = {'USER-AGENT':self._USER_AGENT}
        else:
            self.headers = {'USER-AGENT':user_agent}

    def getNextHeader(self,url):
        """Return the header containing the USER-AGENT field"""
        return self.headers

    def save(self):
        """Required, does nothing"""
        pass


class VisitURLHandler(Handler):
    """Very simple example handler which simply visits the page.
    
    This handler just demonstrates how to interact with the queue.
    """

    def process(self, info, rmi):
        if info['status'] != 200:
            print "putting %s back on queue" % info['url']
            rmi.put(info['url'])
	

if __name__ == '__main__':
    """Basic example of how to start CRAWL-E. The assumption is that
    a queue is running and the url for it is known."""
    runCrawle(sys.argv, handler=VisitURLHandler())
