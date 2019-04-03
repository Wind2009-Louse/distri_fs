import json
import os
import hashlib
import threading
import time
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

nearby_servers = []
timeout_times = {}
fileversion = {}
version_filename = os.path.join(os.getcwd(),"dir.txt")
Lock = threading.Lock()
FLock = {}
TIMEOUT_TIME = 3

def dir_md5(dir):
    return hashlib.md5(bytes(dir,encoding='utf-8')).hexdigest()

def file_realname(dir):
    global self_port
    dir_name = os.path.join(os.getcwd(),"files_%d"%self_port)
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    return os.path.join(dir_name, dir_md5(dir))

def update_version():
    global fileversion
    global self_port
    write_json = json.dumps(fileversion)
    filename = os.path.join(os.getcwd(),"dir_%d.txt"%self_port)
    new_file = open(filename, 'w')
    new_file.write(write_json)
    new_file.close()

# file lock
def acquire_flock(fname):
    if fname not in FLock:
        FLock[fname] = threading.Lock()
    FLock[fname].acquire()
def release_flock(fname):
    if fname not in FLock:
        FLock[fname] = threading.Lock()
        return
    FLock[fname].release()

# connect to main server
def connect_to_server(self_port):
    import xmlrpc.client
    srv = xmlrpc.client.Server('http://localhost:9999/')
    try:
        srv.addsubserver(self_port)
        print("Connect to main server!")
    except Exception as err:
        print(format(err))

# timeout clear thread
class timeout_thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        global timeout_times
        global nearby_servers
        while(1):
            time.sleep(TIMEOUT_TIME)
            Lock.acquire()
            todelete = []
            for _k in timeout_times:
                if timeout_times[_k] >= 3 and _k in nearby_servers:
                    todelete.append(_k)
            for _k in todelete:
                timeout_times.pop(_k)
                nearby_servers.remove(_k)
                print("Delete nearby port %d"%_k)
            Lock.release()

# broadcast thread
class broadcast_thread(threading.Thread):
    def __init__(self, port, version, full_filename, data):
        threading.Thread.__init__(self)
        self.port = port
        self.version = version
        self.full_filename = full_filename
        self.data = data
    def run(self):
        global nearby_servers
        global timeout_times
        import xmlrpc.client
        try:
            srv = xmlrpc.client.Server('http://localhost:%d/' % self.port)
            srv.updateremote(self.version, self.full_filename, self.data)
        except Exception as err:
            print(format(err))
            if self.port not in timeout_times:
                timeout_times[self.port] = 0
            timeout_times[self.port]+=1

# input listen thread
class listen_thread(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
    def run(self):
        global timeout_times
        global nearby_servers
        import xmlrpc.client
        while(1):
            ip = input("Input nearby port or 'check':")
            if ip == 'check':
                print("files: %s"%str(fileversion))
                print("nearby: %s"%str(nearby_servers))
                print("timeout: %s"%str(timeout_times))
                continue
            new_port = -1
            try:
                new_port = int(ip)
                if new_port not in nearby_servers:
                    nearby_servers.append(new_port)
                srv = xmlrpc.client.Server('http://localhost:%d/' % new_port)
                srv.addnearby(self.port)
                # dump connect
                send_json(new_port)
            except:
                if new_port in nearby_servers:
                    nearby_servers.pop(new_port)
                continue

# broadcast new file
def broadcast_file(version, full_filename,data):
    global nearby_servers
    Lock.acquire()
    for _p in nearby_servers:
        _t = broadcast_thread(_p, version, full_filename, data)
        _t.start()
    Lock.release()

def send_json(port):
    global fileversion
    import xmlrpc.client
    srv = xmlrpc.client.Server('http://localhost:%d/' % port)
    srv.updatelist(json.dumps(fileversion))

# RPC

class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

# return 0
# call by other file server
def xmlrpc_addnearby(port):
    Lock.acquire()
    # dump connect
    if port not in nearby_servers:
        nearby_servers.append(port)
    send_json(port)
    Lock.release()
    return 0
# return 0
# call by other file server
def xmlrpc_updatelist(versionjson):
    global fileversion
    global self_port
    tocheck_data = json.loads(versionjson)
    for fullname in fileversion:
        file_localname = os.path.join(os.getcwd(), "files_%d"%self_port, dir_md5(fullname))
        if fullname not in tocheck_data\
                or (fileversion[fullname] > tocheck_data[fullname] and tocheck_data[fullname] != -1)\
                or (fileversion[fullname] == -1 and tocheck_data[fullname] != -1):
            if fileversion[fullname] == -1:
                data = False
            else:
                acquire_flock(fullname)
                open_file = open(file_localname, 'rb')
                data = open_file.read()  # data: bytes
                open_file.close()
                release_flock(fullname)
            broadcast_file(fileversion[fullname], fullname, data)
    return 0
# return 0
# call by server
def xmlrpc_findfile(full_filename):
    global fileversion
    Lock.acquire()
    file_localname = os.path.join(os.getcwd(), "files_%d"%self_port, dir_md5(full_filename))
    if full_filename in fileversion \
        and not os.path.exists(file_localname):
        fileversion.pop(full_filename)
    if full_filename not in fileversion:
        Lock.release()
        return -1
    version = fileversion[full_filename]
    Lock.release()
    return version
# return version, data
# call by client
def xmlrpc_readfile(full_filename):
    global fileversion
    Lock.acquire()
    real_filename = file_realname(full_filename)
    if not os.path.exists(real_filename) or full_filename not in fileversion:
        Lock.release()
        return -1, False
    Lock.release()

    acquire_flock(full_filename)

    if full_filename=="/test.txt":
        while(1):
            a = 1

    open_file = open(real_filename,'rb')
    data = open_file.read() # data: bytes
    open_file.close()
    release_flock(full_filename)

    version = fileversion[full_filename]
    return version, data
# return 0(normal)
# call by server
def xmlrpc_readywrite(full_filename):
    Lock.acquire()
    if full_filename not in fileversion:
        Lock.release()
        return -1
    fileversion[full_filename] += 1
    update_version()
    Lock.release()
    return 0
# return 0(normal)
# call by client
def xmlrpc_writefile(version, full_filename, data):
    Lock.acquire()
    if full_filename in fileversion and fileversion[full_filename] > version:
        Lock.release()
        raise Exception(403, "Your file is too old!")
    real_name = file_realname(full_filename)
    Lock.release()  

    acquire_flock(full_filename)
    file = open(real_name,'wb')
    file.write(data.data)
    file.close()
    release_flock(full_filename)

    Lock.acquire()
    fileversion[full_filename] = max(version, fileversion[full_filename]) + 1
    new_version = fileversion[full_filename]
    update_version()
    Lock.release()
    broadcast_file(new_version, full_filename,data)
    return 0
# return 0(normal)
# call by server
def xmlrpc_createfile(full_filename):
    Lock.acquire()
    if full_filename in fileversion and fileversion[full_filename] != -1:
        Lock.release()
        return -1
    real_name = file_realname(full_filename)
    # just create
    acquire_flock(full_filename)
    file = open(real_name, 'wb')
    file.close()
    release_flock(full_filename)
    fileversion[full_filename] = 1
    update_version()
    Lock.release()
    broadcast_file(1, full_filename, False)
    return 0
# return 0(normal)
# call by server
def xmlrpc_deletefile(full_filename):
    Lock.acquire()
    if full_filename not in fileversion:
        Lock.release()
        return -1
    real_name = file_realname(full_filename)
    # just delete
    os.remove(real_name)
    fileversion[full_filename] = -1
    update_version()
    Lock.release()
    broadcast_file(-1, full_filename, False)
    return 0
# return 0(normal)
# call by other file system
def xmlrpc_updateremote(version, full_filename, data):
    Lock.acquire()
    real_name = file_realname(full_filename)
    # delete
    if version == -1:
        if full_filename in fileversion and fileversion[full_filename] != -1:
            os.remove(real_name)
            fileversion[full_filename] = -1
            update_version()
            Lock.release()
            broadcast_file(-1, full_filename, False)
            return 0
        Lock.release()
        return 0
    # create
    if full_filename not in fileversion\
        or version > fileversion[full_filename]:
        file = open(real_name, 'wb')
        if data != False:
            file.write(data.data)
        file.close()
        fileversion[full_filename] = version
        update_version()
        Lock.release()
        broadcast_file(version, full_filename, data)
    else:
        Lock.release()
    return 0
def xmlrpc_fault(self):
    raise Exception("Unknown error.")

if __name__ == '__main__':

    self_port = int(input("Input open port:"))
    connect_to_server(self_port)
    filename = os.path.join(os.getcwd(),"dir_%d.txt"%self_port)

    # load fileversion
    if os.path.exists(filename):
        read_file = open(filename,'r')
        data = read_file.read()
        read_file.close()
        fileversion = json.loads(data)
    else:
        json_data = json.dumps(fileversion)
        new_file = open(filename, 'w')
        new_file.write(json_data)
        new_file.close()
    
    # acquire a lock for each file
    for k,v in fileversion.items():
        if v != -1:
            FLock[k] = threading.Lock()

    # timeout thread
    to_thread = timeout_thread()
    to_thread.start()

    # listen thread
    listen = listen_thread(self_port)
    listen.start()

    # rpc system
    server = ThreadXMLRPCServer(('localhost', self_port)) # 初始化

    server.register_function(xmlrpc_addnearby, "addnearby")
    server.register_function(xmlrpc_updatelist, "updatelist")
    server.register_function(xmlrpc_findfile, "findfile")
    server.register_function(xmlrpc_readfile, "readfile")
    server.register_function(xmlrpc_readywrite, "readywrite")
    server.register_function(xmlrpc_writefile, "writefile")
    server.register_function(xmlrpc_createfile, "createfile")
    server.register_function(xmlrpc_deletefile, "deletefile")
    server.register_function(xmlrpc_updateremote, "updateremote")

    server.serve_forever() # 保持等待调用状态
