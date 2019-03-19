from twisted.web import xmlrpc, server
import json
import os
import hashlib
import threading
import time

nearby_servers = []
timeout_times = {}
fileversion = {}
version_filename = os.path.join(os.getcwd(),"dir.txt")
Lock = threading.Lock()
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
class filesystem(xmlrpc.XMLRPC):
    # return 0
    # call by other file server
    def xmlrpc_addnearby(self, port):
        Lock.acquire()
        # dump connect
        if port not in nearby_servers:
            nearby_servers.append(port)
        send_json(port)
        Lock.release()
        return 0
    # return 0
    # call by other file server
    def xmlrpc_updatelist(self, versionjson):
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
                    open_file = open(file_localname, 'rb')
                    data = open_file.read()  # data: bytes
                    open_file.close()
                broadcast_file(fileversion[fullname], fullname, data)
        return 0
    # return 0
    # call by server
    def xmlrpc_findfile(self, full_filename):
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
    def xmlrpc_readfile(self, full_filename):
        global fileversion
        Lock.acquire()
        real_filename = file_realname(full_filename)
        if not os.path.exists(real_filename) or full_filename not in fileversion:
            Lock.release()
            return -1, False
        open_file = open(real_filename,'rb')
        data = open_file.read() # data: bytes
        open_file.close()
        version = fileversion[full_filename]
        Lock.release()
        return version, data
    # return 0(normal)
    # call by server
    def xmlrpc_readywrite(self, full_filename):
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
    def xmlrpc_writefile(self, version, full_filename, data):
        Lock.acquire()
        if full_filename in fileversion and fileversion[full_filename] > version:
            Lock.release()
            raise xmlrpc.Fault(403, "Your file is too old!")
        real_name = file_realname(full_filename)
        file = open(real_name,'wb')
        file.write(data.data)
        file.close()
        fileversion[full_filename] = max(version, fileversion[full_filename]) + 1
        new_version = fileversion[full_filename]
        update_version()
        Lock.release()
        broadcast_file(new_version, full_filename,data)
        return 0
    # return 0(normal)
    # call by server
    def xmlrpc_createfile(self, full_filename):
        Lock.acquire()
        if full_filename in fileversion and fileversion[full_filename] != -1:
            Lock.release()
            return -1
        real_name = file_realname(full_filename)
        # just create
        file = open(real_name, 'wb')
        file.close()
        fileversion[full_filename] = 1
        update_version()
        Lock.release()
        broadcast_file(1, full_filename, False)
        return 0
    # return 0(normal)
    # call by server
    def xmlrpc_deletefile(self, full_filename):
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
    def xmlrpc_updateremote(self, version, full_filename, data):
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
    from twisted.internet import reactor

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

    # timeout thread
    to_thread = timeout_thread()
    to_thread.start()

    # listen thread
    listen = listen_thread(self_port)
    listen.start()

    # rpc system
    srv = filesystem()
    reactor.listenTCP(self_port, server.Site(srv))
    reactor.run()
