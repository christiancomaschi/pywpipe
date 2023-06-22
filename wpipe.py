import ctypes
import ctypes.wintypes
import threading
import queue
import struct
import time
import sys
import contextlib
import logging

hk32 = ctypes.windll.LoadLibrary('kernel32.dll')

PIPE_ACCESS_DUPLEX =                    0x00000003
FILE_FLAG_FIRST_PIPE_INSTANCE =         0x00080000
FILE_FLAG_OVERLAPPED =         0x40000000
PIPE_TYPE_BYTE =                        0x00000000
PIPE_TYPE_MESSAGE =                     0x00000004
PIPE_READMODE_MESSAGE =                 0x00000002
OPEN_EXISTING =                         0x00000003
GENERIC_READ =                          0x80000000
GENERIC_WRITE =                         0x40000000

if sys.maxsize > 2**32:
    def ctypes_handle(handle):
        return ctypes.c_ulonglong(handle)
else:
    def ctypes_handle(handle):
        return ctypes.c_uint(handle)

logger = logging.getLogger('wpipe')
        
class Mode:
    Master =            0
    Slave =             1
    Reader =            2
    Writer =            3
    SingleTransaction = 4

    def is_slave(mode):
        return mode & 3 == Mode.Slave
    def is_master(mode):
        return mode & 3 == Mode.Master
    def is_reader(mode):
        return mode & 3 == Mode.Reader
    def is_writer(mode):
        return mode & 3 == Mode.Writer
    def is_strans(mode):
        #return mode & Mode.SingleTransaction == Mode.SingleTransaction
        return True


class OVERLAPPED(ctypes.Structure):
    _fields_ = [("Internal", ctypes.c_int),
                ("InternalHigh", ctypes.c_int),
                ("Offset", ctypes.wintypes.DWORD),
                ("OffsetHigh", ctypes.wintypes.DWORD),
                ("hEvent", ctypes.wintypes.HANDLE)]
                
@contextlib.contextmanager
def lock_with_timeout(lock, timeout=-1):
    acquired = lock.acquire(True, timeout)
    if acquired:
        yield acquired
        lock.release()
    else:
        yield acquired

class Base:
    def asyncreader(self,nph,buf,hEvent,timeout): # yields (num_bytes_read_if_completed, bool_has_completed)
        ovrlpd=OVERLAPPED(0, 0, 0, 0, hEvent)
        completed=False
        cnt = b'\x00\x00\x00\x00'
        logger.debug('Rgen: Read')
        ret = hk32['ReadFile'](
            ctypes_handle(nph), buf, 4096, ctypes.c_char_p(cnt), ctypes.byref(ovrlpd)
        )

        logger.debug('Rgen: Ret='+str(ret))
        if ret != 0:
            completed = True
            cnt = struct.unpack('I', cnt)[0]
            logger.debug('Rgen: Completed: cnt='+str(cnt))
        else:
            err = hk32['GetLastError']()
            if err == 109: # broken pipe
                ovrlpd=None
                logger.debug('Rgen: broken pipe')
                yield 0, True
                return 
            elif err != 997: # ERROR_IO_PENDING
                ovrlpd=None
                logger.debug('Rgen: Err unknown ('+str(err)+')')
                yield -1, True
                return

        if completed:
            yield cnt, True
        while not completed:
            # get ReadFile completion status
            logger.debug('Rgen: Wait hEvent')
            ret=hk32['WaitForSingleObject'](ctypes.c_uint(hEvent), timeout)
            if ret == 0: # signaled event
                logger.debug('Rgen: Signaled: get overlapped result')
                ret=hk32['GetOverlappedResult'](ctypes.c_uint(nph), ctypes.byref(ovrlpd), ctypes.c_char_p(cnt), 0)
                if ret!=0:
                    ovrlpd=None
                    cnt = struct.unpack('I', cnt)[0]
                    completed=True
                    logger.debug('Rgen: Completed: cnt='+str(cnt))
                    yield cnt, True
                else:
                    err = hk32['GetLastError']()
                    if (err == 997) or (err == 258): # first time pending, then time out
                        logger.debug('Rgen: Err=pending/timeout')
                        yield 0, False
                    elif err == 995: # aborted => StopIteration
                        logger.debug('Rgen: Aborted')
                        ovrlpd=None
                        return
                    elif err == 109: # broken pipe
                        ovrlpd=None
                        logger.debug('Rgen: broken pipe')
                        yield 0, True
                        return 
                    else:
                        logger.debug('Rgen: Err=unknown ('+str(err)+')')
                        ovrlpd=None
                        yield -1, True
                        return
            elif ret == 0x102: # timeout
                logger.debug('Rgen: hEvent timeout')
                yield 0, False
            else:
                logger.debug('Rgen: Wait err=unknown ('+str(err)+')')
                ovrlpd=None
                yield -1, True
                return


    def asyncwriter(self,nph,rawmsg,hEvent,timeout): # yields (num_bytes_written_if_completed, bool_has_completed)
        ovrlpd=OVERLAPPED(0, 0, 0, 0, hEvent)
        completed=False
        cnt = b'\x00\x00\x00\x00'
        logger.debug('Wgen: Write')
        ret = hk32['WriteFile'](
            ctypes_handle(nph), ctypes.c_char_p(rawmsg), 
            ctypes.c_uint(len(rawmsg)), 
            ctypes.c_char_p(cnt),
            ctypes.byref(ovrlpd)
        )

        logger.debug('Wgen: Ret='+str(ret))
        if ret != 0:
            completed = True
            cnt = struct.unpack('I', cnt)[0]
            logger.debug('Wgen: Completed: cnt='+str(cnt))
        else:
            err = hk32['GetLastError']()
            if err == 109: # broken pipe
                ovrlpd=None
                logger.debug('Wgen: broken pipe')
                yield 0, True
                return 
            elif err != 997: # ERROR_IO_PENDING
                ovrlpd=None
                logger.debug('Wgen: Err=pending')
                yield -1, True
                return

        if completed:
            logger.debug('Wgen: Completed')
            yield cnt, True
        while not completed:
            # get WriteFile completion status
            logger.debug('Wgen: Wait hEvent')
            ret=hk32['WaitForSingleObject'](ctypes.c_uint(hEvent), timeout)
            if ret == 0: # signaled event
                logger.debug('Wgen: Signaled: get overlapped result')
                ret=hk32['GetOverlappedResult'](ctypes.c_uint(nph), ctypes.byref(ovrlpd), ctypes.c_char_p(cnt), 0)
                if ret!=0:
                    ovrlpd=None
                    cnt = struct.unpack('I', cnt)[0]
                    completed=True
                    logger.debug('Wgen: Completed')
                    yield cnt, True
                else:
                    err = hk32['GetLastError']()
                    if (err == 997) or (err == 258): # first time pending, then time out
                        logger.debug('Wgen: Err=pending/timeout')
                        yield 0, False
                    elif err == 995: # aborted => StopIteration
                        ovrlpd=None
                        logger.debug('Wgen: Aborted')
                        return
                    elif err == 109: # broken pipe
                        ovrlpd=None
                        logger.debug('Wgen: broken pipe')
                        yield 0, True
                        return 
                    else:
                        ovrlpd=None
                        logger.debug('Wgen: Err=unknown ('+str(err)+')')
                        yield -1, True
                        return
            elif ret == 0x102: # timeout
                logger.debug('Wgen: Wait timeout')
                yield 0, False
            else:
                ovrlpd=None
                logger.debug('Wgen: Err=unknown ('+str(err)+')')
                yield -1, True
                return


    def readerentry(self, nph, client, mode, server):
        rq = client.rq
        wq = client.wq

        buf = ctypes.create_string_buffer(4096)

        asyncreader_gen = None
        cnt = 0
        completed = False
        hEvent = None
        rlock_acquiring=False
        while (client.alive) and ((server is None) or (not server.must_shutdown)):
            '''
                In master mode we wait to start trying to read until
                we get issue a command since trying to read here would
                block any writes. So we are released once one or more
                writes have been completed. If we never need to read
                then we should be using Mode.Writer not Mode.Master.
            '''
            if not rlock_acquiring:
                if mode == Mode.Master:
                    with client.rwait:
                        while not client.rwait.wait(timeout=0.1):
                            logger.debug('Rthread: wait for write in master mode')
                            if (not client.alive) or ((server is not None) and (server.must_shutdown)):
                                client.alive = False
                                logger.debug('rthread: closed while waiting for write in master mode')
                                return

                hEvent = hk32['CreateEventA'](None, 1, 0, None)
                asyncreader_gen = self.asyncreader(nph, buf, hEvent, 100)
                cnt = 0
                completed = False
            
            logger.debug('Rthread: request lock for read')
            with lock_with_timeout(client.rlock, 0.1) as acquired:
                if acquired:
                    logger.debug('Rthread: lock for read acquired')
                    rlock_acquiring = False
                    while not completed:
                        with client.io_rlock: # timeout not necessary: shared only with CancelIO call
                            logger.debug('Rthread: I/O calls lock acquired')
                            try:
                                logger.debug('Rthread: call Asyncreader_gen')
                                cnt, completed = next(asyncreader_gen)
                            except StopIteration: # I/O canceled
                                logger.debug('Rthread: StopIteration')
                                hk32['CloseHandle'](ctypes.c_uint(hEvent))
                                client.alive = False
                                return

                        if ((not client.alive) or ((server is not None) and (server.must_shutdown))) or \
                           (cnt == -1): # unexpected error
                            logger.debug('Rthread: close/shutdown')
                            asyncreader_gen.close()
                            hk32['CloseHandle'](ctypes.c_uint(hEvent))
                            client.alive = False
                            if (server is not None) and (cnt==-1):
                                server.must_shutdown = True
                            return

                else: # timeout, still acquiring rlock
                    logger.debug('Rthread: lock request timed out')
                    if (not client.alive) or ((server is not None) and (server.must_shutdown)):
                        logger.debug('Rthread: close/shutdown')
                        asyncreader_gen.close()
                        hk32['CloseHandle'](ctypes.c_uint(hEvent))
                        client.alive = False
                        rlock_acquiriing = False
                        return
                        
                    rlock_acquiring = True
                    logger.debug('Rthread: lock retry')
                    continue
                    
            logger.debug('Rthread: close hEvent and asyncreader_gen')
            if hEvent is not None:
                hk32['CloseHandle'](ctypes.c_uint(hEvent))
            if asyncreader_gen is not None:
                asyncreader_gen.close()

            if (completed and (cnt == 0)) or \
               (not client.alive) or \
               ((server is not None) and server.must_shutdown):
                logger.debug('Rthread: close for dead pipe or close request')
                rq.put(None)                    # signal reader that pipe is dead
                wq.put(None)                    # signal write thread to terminate
                client.alive = False
                return

            logger.debug('Rthread: put data into queue')
            rawmsg = buf[0:cnt]
            rq.put(rawmsg)

            client.pendingread = False

            if server is not None:
                server.hasdata = True

            '''
                In slave mode we wait after reading so that we may be able
                to write a reply if needed. If we never need any replies
                then we should be using Mode.Reader instead of Mode.Slave.
            '''
            if Mode.is_slave(mode):
                with client.rwait:
                    logger.debug('Rthread: wait for write in slave mode')
                    while not client.rwait.wait(timeout=0.1):
                        if (not client.alive) or ((server is not None) and (server.must_shutdown)):
                            logger.debug('Rthread: close/shutdown request during write wait in slave mode')
                            return

    def writerentry(self, nph, client, mode, server):
        wq = client.wq

        asyncwriter_gen = None
        written = 0
        completed = False
        hEvent=None

        while True:
            try:
                logger.debug('Wthread: get message from queue')
                rawmsg = wq.get(timeout=0.1)
            except queue.Empty: # timeout
                logger.debug('Wthread: queue time out')
                if (not client.alive) or ((server is not None) and (server.must_shutdown)):
                    client.alive = False
                    logger.debug('Wthread: close/shutdown')
                    return

                logger.debug('Wthread: retry get data from queue')
                continue

            if rawmsg is None:
                client.alive = False
                logger.debug('Wthread: write None - close/shutdown')
                return

            hEvent = hk32['CreateEventA'](None, 1, 0, None)
            asyncwriter_gen = self.asyncwriter(nph, rawmsg, hEvent, 100)
            written = 0
            completed = False
        
            while not completed:
                logger.debug('Wthread: request lock for I/O calls')
                with client.io_rlock: # timeout not necessary: shared only with CancelIO call
                    try:
                        logger.debug('Wthread: call asyncwriter_gen')
                        written, completed = next(asyncwriter_gen)
                    except StopIteration: # I/O canceled
                        logger.debug('Wthread: StopIteration')
                        hk32['CloseHandle'](ctypes.c_uint(hEvent))
                        client.alive = False
                        return

                if ((not client.alive) or ((server is not None) and (server.must_shutdown))) or \
                   (written == -1): # unexpected error
                    logger.debug('Wthread: shutdown/close')
                    asyncwriter_gen.close()
                    hk32['CloseHandle'](ctypes.c_uint(hEvent))
                    client.alive = False
                    if (server is not None) and (written == -1):
                        logger.debug('Wthread: reason=unexpected error')
                        server.must_shutdown = True
                    return

            logger.debug('Wthread: close hEvent and asyncwriter_gen')
            if hEvent is not None:
                hk32['CloseHandle'](ctypes.c_uint(hEvent))
            if asyncwriter_gen is not None:
                asyncwriter_gen.close()

            if (completed and (written == 0)) or \
               (not client.alive) or \
               ((server is not None) and server.must_shutdown):
                logger.debug('Wthread: dead pipe or close/shutdown request')
                rq.put(None)                    # signal reader that pipe is dead
                client.alive = False
                return

            if (Mode.is_slave(mode) or Mode.is_master(mode)) and Mode.is_strans(mode):
                logger.debug('Wthread: slave mode - end transaction')
                client.endtransaction()


class ServerClient:
    def __init__(self, handle, mode, maxmessagesz):
        self.handle = handle
        self.rq = queue.Queue()
        self.wq = queue.Queue()
        self.alive = True
        self.mode = mode
        self.maxmessagesz = maxmessagesz
        # keep thread objects to call join before closing handles
        self.rthread=None
        self.wthread=None
        '''

        '''
        self.rwait = threading.Condition()
        '''
            The `pendingread` serves to prevent you from writing 
            again before getting a reply.
        '''
        self.pendingread = False
        '''
            The `rlock` serves to prevents you from issuing a write
            while a read operation is blocking.
        '''
        self.rlock = threading.Lock()

        '''
            The `io_rlock` serves to prevent you from calling I/O functions while or
            after CancelIO has been called
        '''
        self.io_rlock = threading.Lock()

    def isalive(self):
        return self.alive

    def endtransaction(self):
        with self.rwait:
            self.rwait.notify()

    def read(self):
        # only throw exception if no data can be read
        if not self.alive and not self.canread():
            raise Exception('Pipe is dead!')
        if Mode.is_writer(self.mode):
            raise Exception('This pipe is in write mode!')

        return self.rq.get()

    def write(self, message):
        if not self.alive:
            raise Exception('Pipe is dead!')
        if Mode.is_reader(self.mode):
            raise Exception('This pipe is in read mode!')
        if Mode.is_slave(self.mode) and not self.rlock.acquire(blocking = False):
            raise Exception('The pipe is currently being read!')
        if Mode.is_master(self.mode) and self.pendingread:
            raise Exception('Master mode must wait for slave reply!')

        self.pendingread = True
        self.wq.put(message)
        if Mode.is_slave(self.mode):
            self.rlock.release()
        return True

    def canread(self):
        return not self.rq.empty()

    def close(self):
        hk32['CancelIoEx'](ctypes_handle(self.handle), 0)
        self.alive = False
        self.rthread.join()
        self.wthread.join()
        hk32['CloseHandle'](ctypes_handle(self.handle))

class Client(Base):
    def __init__(self, name, mode, *, maxmessagesz = 4096):
        self.mode = mode
        self.maxmessagesz = maxmessagesz
        self.name = name
        self.handle = hk32['CreateFileA'](
            ctypes.c_char_p(b'\\\\.\\pipe\\' + bytes(name, 'utf8')),
            ctypes.c_uint(GENERIC_READ | GENERIC_WRITE),
            0,                      # no sharing
            0,                      # default security
            ctypes.c_uint(OPEN_EXISTING),
            0,                      # default attributes
            0                       # no template file
        )


        if hk32['GetLastError']() != 0:
            err = hk32['GetLastError']()
            self.alive = False
            raise Exception('Pipe Open Failed [%s]' % err)
            return

        xmode = struct.pack('I', PIPE_READMODE_MESSAGE)
        ret = hk32['SetNamedPipeHandleState'](
            ctypes_handle(self.handle),
            ctypes.c_char_p(xmode),
            ctypes.c_uint(0),
            ctypes.c_uint(0)
        )

        if ret == 0:
            err = hk32['GetLastError']()
            self.alive = False
            raise Exception('Pipe Set Mode Failed [%s]' % err)
            return

        self.client = ServerClient(self.handle, self.mode, self.maxmessagesz)

        if not Mode.is_writer(self.mode):
            self.client.rthread = threading.Thread(target = self.readerentry, args = (self.handle, self.client, self.mode, None))
            self.client.rthread.start()

        if not Mode.is_reader(self.mode):
            self.client.wthread = threading.Thread(target = self.writerentry, args = (self.handle, self.client, self.mode, None))
            self.client.wthread.start()

        self.alive = True
        return

    def endtransaction(self):
        self.client.endtransaction()

    def close(self):
        self.client.close()

    def read(self):
        return self.client.read()

    def write(self, message):
        if not self.alive:
            raise Exception('Pipe Not Alive')
        return self.client.write(message)

class Server(Base):
    def __init__(self, name, mode, *, maxclients = 5, maxmessagesz = 4096, maxtime = 100):
        self.name = name
        self.mode = mode
        self.clients = []
        self.maxclients = maxclients
        self.maxmessagesz = 4096
        self.maxtime = maxtime
        self.must_shutdown = False
        self.t = threading.Thread(target = self.serverentry)
        self.t.start()
        self.hasdata = False

    def dropdeadclients(self):
        toremove = []
        for client in self.clients:
            if not client.alive and not client.canread():
                toremove.append(client)
        for client in toremove:
            client.close()
            self.clients.remove(client)

    def getclientcount():
        self.dropdeadclients()
        return len(self.clients)

    def getclient(self, index):
        return self.clients[index]

    def __iter__(self):
        for client in self.clients:
            yield client

    def __index__(self, index):
        return self.clients[index]

    def shutdown(self):
        for client in self.clients:
            client.close()
        toremove = []
        self.must_shutdown = True

    def waitfordata(self, timeout = None, interval = 0.01):
        if self.hasdata:
            self.hasdata = False
            return True

        st = time.time()
        while not self.hasdata:
            if timeout is not None and time.time() - st > timeout:
                return False
            time.sleep(interval)
        self.hasdata = False
        return True

    def serverentry(self):
        while not self.must_shutdown:
            self.dropdeadclients()

            logger.debug('Srv: Create named pipe')
            nph = hk32['CreateNamedPipeA'](
                ctypes.c_char_p(b'\\\\.\\pipe\\' + bytes(self.name, 'utf8')),
                ctypes.c_uint(PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED),
                ctypes.c_uint(PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE),
                ctypes.c_uint(self.maxclients),
                ctypes.c_uint(self.maxmessagesz), ctypes.c_uint(self.maxmessagesz),
                ctypes.c_uint(self.maxtime),
                ctypes.c_uint(0)
            )

            err = hk32['GetLastError']()

            '''
                ERROR_PIPE_BUSY, we have used up all instances
                of the pipe and therefore must wait until one
                before free
            '''
            if err == 231:
                time.sleep(2)
                continue

            hEvent=None
            ovrlpd=None
            connecting=False
            while not self.must_shutdown:
                if not connecting:
                    # wait for connection
                    logger.debug('Srv: Create hEvent for ConnectNamedPipe')
                    hEvent = hk32['CreateEventA'](None, 1, 0, None)
                    if hEvent == 0:
                        hEvent=None
                        must_shotdown=True
                        break

                    ovrlpd=OVERLAPPED(0, 0, 0, 0, hEvent)
                    logger.debug('Srv: Call ConnectNamedPipe')
                    ret = hk32['ConnectNamedPipe'](ctypes.c_uint(nph), ctypes.byref(ovrlpd))

                    if ret == 0:
                        err = hk32['GetLastError']()
                        if err == 997: # ERROR_IO_PENDING
                            logger.debug('Srv: Err=pending')
                            connecting = True
                            continue
                        elif err == 535: # already connected
                            logger.debug('Srv: Err=already connected')
                            hk32['CloseHandle'](ctypes.c_uint(hEvent))
                            hEvent=None
                            ovrlpd=None
                            break
                        else: # Something went wrong
                            logger.debug('Srv: Err=unknown ('+str(err)+')')
                            hk32['CloseHandle'](ctypes.c_uint(hEvent))
                            hk32['CancelIo'](ctypes.c_uint(nph))
                            hk32['CloseHandle'](ctypes.c_uint(nph))
                            hEvent=None
                            nph=None
                            ovrlpd=None
                            must_shutdown=True
                            break

                    else: # async ConnectNamedPipe should not return nonzeror value
                        logger.debug('Srv: ConnectNamedPipe: unknown return value')
                        hk32['CloseHandle'](ctypes.c_uint(hEvent))
                        hk32['CancelIo'](ctypes.c_uint(nph))
                        hk32['CloseHandle'](ctypes.c_uint(nph))
                        hEvent=None
                        nph=None
                        ovrlpd=None
                        must_shutdown = True
                        break
                        
                else:
                    # get pending connection completion status
                    logger.debug('Srv: Wait for connection hEvent')
                    ret=hk32['WaitForSingleObject'](ctypes.c_uint(hEvent), self.maxtime)
                    if ret == 0x102: # timeout
                        logger.debug('Srv: Connection hEvent time out')
                        continue
                    elif (ret != 0x102) and (ret != 0): # nor timeout nor singlaed event: unexpected error
                        logger.debug('Srv: Unknown wait error')
                        hk32['CloseHandle'](ctypes.c_uint(hEvent))
                        hk32['CancelIo'](ctypes.c_uint(nph))
                        hk32['CloseHandle'](ctypes.c_uint(nph))
                        hEvent=None
                        nph=None
                        ovrlpd=None
                        must_shutdown=True
                        break

                    logger.debug('Srv: ConnectNamedPipe: get overlapped result')
                    dummy = b'\x00\x00\x00\x00'
                    ret=hk32['GetOverlappedResult'](ctypes.c_uint(nph), ctypes.byref(ovrlpd), ctypes.c_char_p(dummy), 0)
                    if ret!=0:
                        logger.debug('Srv: Connected')
                        hk32['CloseHandle'](ctypes.c_uint(hEvent))
                        hEvent=None
                        ovrlpd=None
                        connecting=False
                        break
                    else:
                        err = hk32['GetLastError']()
                        if (err == 997) or (err == 258): # first time pending, then time out
                            logger.debug('Srv: Err=pending/timeout: retry')
                            continue

            if self.must_shutdown:
                logger.debug('Srv: Shutdown requested: close connection hEvent and pipe handle')
                if hEvent is not None:
                    hk32['CloseHandle'](ctypes.c_uint(hEvent))
                    hEvent=None
                if nph is not None:
                    hk32['CancelIo'](ctypes.c_uint(nph))
                    hk32['CloseHandle'](ctypes.c_uint(nph))
                    nph=None
                ovrlpd=None
                break
                            
            logger.debug('Srv: Create threads')
            client = ServerClient(nph, self.mode, self.maxmessagesz)

            if self.mode != Mode.Writer:
                client.rthread = threading.Thread(target = self.readerentry, args = (nph, client, self.mode, self))
                client.rthread.start()

            if self.mode != Mode.Reader:
                client.wthread = threading.Thread(target = self.writerentry, args = (nph, client, self.mode, self))
                client.wthread.start()

            self.clients.append(client)

def getpipepath(name):
    return '\\\\.\\pipe\\' + name