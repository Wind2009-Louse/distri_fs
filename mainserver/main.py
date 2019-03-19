import json
import os
import time
import threading
from twisted.web import xmlrpc, server

file_dirs = {"/":[]}
BACKUP_TIME = 10
TIMEOUT_TIME = 10
Lock = threading.Lock()

subserver_port = []
timeout_times = {}
deleting_file = []

dir_filename = os.path.join(os.getcwd(),"dir.txt")

def update_dirfile():
    global file_dirs
    write_json = json.dumps(file_dirs)
    new_file = open(dir_filename, 'w')
    new_file.write(write_json)
    new_file.close()

def get_parent_dir(dir):
    if dir[len(dir)-1] != '/':
        dir += '/'
    dir_split = dir.split('/')
    new_dir = "/"
    for i in range(1, len(dir_split) - 2):
        if dir_split[i] != '':
            new_dir += dir_split[i]
            new_dir += '/'
    return new_dir, dir_split[len(dir_split)-2]

# backup thread
class dir_backup(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.last_dir = {}
    def run(self):
        global file_dirs
        while(1):
            time.sleep(BACKUP_TIME)
            Lock.acquire()
            if self.last_dir != file_dirs:
                self.last_dir = file_dirs
                Lock.release()
                write_json = json.dumps(self.last_dir)
                current_time = time.strftime("%Y-%m-%d %H-%M-%S.txt", time.localtime())
                mf_dir = os.path.join(os.getcwd(), "backup")
                if not os.path.exists(mf_dir):
                    os.mkdir(mf_dir)
                back_dir = os.path.join(os.getcwd(), "backup", current_time)
                back_file = open(back_dir,'w')
                back_file.write(write_json)
                back_file.close()
            else:
                Lock.release()

# timeout clear thread
class timeout_thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        global timeout_times
        global subserver_port
        while(1):
            time.sleep(TIMEOUT_TIME)
            Lock.acquire()
            todelete = []
            for _k in timeout_times:
                if timeout_times[_k] >= 3 and _k in subserver_port:
                    todelete.append(_k)
            for _k in todelete:
                timeout_times.pop(_k)
                subserver_port.remove(_k)
                print("Delete nearby port %d"%_k)
            Lock.release()

# ask version thread
class version_thread(threading.Thread):
    def __init__(self, port, full_filename, result_list):
        threading.Thread.__init__(self)
        self.port = port
        self.full_filename = full_filename
        self.result_list = result_list
    def run(self):
        global timeout_times
        global file_dirs
        try:
            import xmlrpc.client
            srv = xmlrpc.client.Server('http://localhost:%d/'%self.port)
            version = srv.findfile(self.full_filename)
            self.result_list[self.port] = version
        except:
            if self.port not in timeout_times:
                timeout_times[self.port] = 0
            timeout_times[self.port]+=1

# delete file thread
class delete_thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        while(1):
            Lock.acquire()
            current_delete = deleting_file.copy()
            deleting_file.clear()
            Lock.release()
            cannot_delete = []
            for _f in current_delete:
                deleted = False
                for _p in subserver_port:
                    try:
                        import xmlrpc.client
                        srv = xmlrpc.client.Server('http://localhost:%d/' % _p)
                        srv.deletefile(_f)
                        deleted = True
                        break
                    except:
                        if _p not in timeout_times:
                            timeout_times[_p] = 0
                        timeout_times[_p] += 1
                        continue
                if not deleted:
                    cannot_delete.append(_f)
            if len(cannot_delete) > 0:
                Lock.acquire()
                for _f in cannot_delete:
                    deleting_file.append(_f)
                Lock.release()

def create_file(port, full_filename):
    import xmlrpc.client
    srv = xmlrpc.client.Server('http://localhost:%d/' % port)
    return srv.createfile(full_filename)

def get_versions_from_server(full_filename):
    result_list = {}
    thread_list = []
    for port in subserver_port:
        new_thread = version_thread(port, full_filename, result_list)
        new_thread.start()
        thread_list.append(new_thread)
    for thr in thread_list:
        thr.join()
    return result_list

# RPC
class filedirsystem(xmlrpc.XMLRPC):
    def xmlrpc_addsubserver(self, port):
        if port not in subserver_port:
            subserver_port.append(port)
        print("New server: %d" % port)
        return 0
    def xmlrpc_dir(self, dir):
        Lock.acquire()
        dir_cpy = file_dirs
        Lock.release()
        print("%sdir"%dir)
        if dir in dir_cpy:
            dir_list = []
            file_list = []
            for f in dir_cpy[dir]:
                if f[len(f)-1] == '/':
                    dir_list.append(f)
                else:
                    file_list.append(f)
            return_str = ""
            for f in dir_list:
                return_str += (f + "\n")
            for f in file_list:
                return_str += (f + "\n")
            return return_str
        else:
            raise xmlrpc.Fault(404, "No such directory.")
    def xmlrpc_mkdir(self, dir, newdir):
        if len(newdir) == 0:
            return 0
        Lock.acquire()
        if newdir[len(newdir)-1] != '/':
            newdir += '/'
        print("%smkdir %s"%(dir,newdir))
        if dir in file_dirs:
            if newdir in file_dirs[dir]:
                Lock.release()
                raise xmlrpc.Fault(403, "There're already the same directory in %s while creating %s."%(dir,newdir))
            file_dirs[dir].append(newdir)
            file_dirs[dir+newdir] = []
            update_dirfile()
            Lock.release()
            return 0
        else:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s." % dir)
    def xmlrpc_cd(self, dir, cmd):
        Lock.acquire()
        dir_cpy = file_dirs
        Lock.release()
        print("%scd %s" % (dir, cmd))
        if cmd == '..':
            new_dir = get_parent_dir(dir)[0]
            if new_dir not in dir_cpy:
                raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s."%new_dir)
            return new_dir
        else:
            if len(cmd)==0:
                raise xmlrpc.Fault(403, "Forbidden operation.")
            if cmd[len(cmd)-1]!='/':
                cmd+='/'
            if cmd[0] != '/':
                new_dir = dir + cmd
            else:
                new_dir = cmd
            if new_dir not in dir_cpy:
                raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s." % new_dir)
            else:
                return new_dir
    def xmlrpc_rm(self, dir, cmd):
        if len(cmd)==0:
            raise xmlrpc.Fault(403, "Forbidden operation.")
        if cmd == '/':
            raise xmlrpc.Fault(403, "You can't delete root.")
        Lock.acquire()
        print("%srm %s" % (dir, cmd))
        # make cmd delete from root
        is_delete_dir = cmd[len(cmd)-1] == '/'
        if cmd[0] != '/':
            cmd = dir + cmd
        # delete directory from root
        if is_delete_dir:
            if cmd not in file_dirs:
                Lock.release()
                raise xmlrpc.Fault(404, "You're trying to delete directory which doesn't exists: %s." % cmd)
            # delete everything from the directory
            todelete = []
            for _k in file_dirs.keys():
                if _k[0:len(cmd)] == cmd:
                    todelete.append(_k)
            for _k in todelete:
                for _f in file_dirs[_k]:
                    if (_f[len(_f)-1] != '/'):
                        deleting_file.append((_k + _f))
                file_dirs.pop(_k)
            # get return dir
            parent, name = get_parent_dir(cmd)
            name += '/'
            if parent not in file_dirs or name not in file_dirs[parent]:
                Lock.release()
                raise xmlrpc.Fault(1, "Error occured while delete file.")
            file_dirs[parent].remove(name)
            # get return dir
            if dir not in file_dirs:
                return_dir = "/"
            else:
                return_dir = dir
            update_dirfile()
            Lock.release()
            return return_dir
        # delete file
        else:
            parent, name = get_parent_dir(cmd)
            if parent in file_dirs and name in file_dirs[parent]:
                file_dirs[parent].remove(name)
                deleting_file.append((parent + name))
                update_dirfile()
                Lock.release()
                return dir
            else:
                Lock.release()
                raise xmlrpc.Fault(404, "You're trying to delete file which doesn't exists: %s." % cmd)
    def xmlrpc_create(self, dir, cmd):
        if len(cmd) == 0:
            raise xmlrpc.Fault(403, "Forbidden Operation.")
        if len(subserver_port) == 0:
            raise xmlrpc.Fault(404, "No live server.")
        if cmd[0] == '/':
            full_filename = cmd
        else:
            full_filename = dir + cmd
        Lock.acquire()
        print("%smf %s" % (dir, cmd))
        parent, filename = get_parent_dir(full_filename)
        if parent not in file_dirs:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s." % parent)
        if filename in file_dirs[parent]:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to create a file that already exists: %s." % full_filename)
        result_list = get_versions_from_server(full_filename)
        if len(result_list) == 0:
            Lock.release()
            raise xmlrpc.Fault(404, "No live server.")
        sort_result = sorted(result_list)
        newest_port = sort_result[0]
        if result_list[newest_port] != -1:
            Lock.release()
            raise xmlrpc.Fault(404, "There're already such file in the system.")
        try:
            result = create_file(newest_port, full_filename)
            file_dirs[parent].append(filename)
            update_dirfile()
            Lock.release()
            return result
        except:
            Lock.release()
            raise xmlrpc.Fault(404, "Error occured.")
    def xmlrpc_read(self, dir, cmd):
        if len(cmd) == 0:
            raise xmlrpc.Fault(403, "Forbidden Operation.")
        if len(subserver_port) == 0:
            raise xmlrpc.Fault(404, "No live server.")
        if cmd[0] == '/':
            full_filename = cmd
        else:
            full_filename = dir + cmd
        Lock.acquire()
        print("%sopen %s" % (dir, cmd))
        parent, filename = get_parent_dir(full_filename)
        if parent not in file_dirs:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s." % parent)
        if filename not in file_dirs[parent]:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to read a file that doesn't exists: %s." % full_filename)
        result_list = get_versions_from_server(full_filename)
        if len(result_list) == 0:
            Lock.release()
            raise xmlrpc.Fault(404, "No live server.")
        sort_result = sorted(result_list)
        newest_port = sort_result[0]
        if result_list[newest_port] == -1:
            Lock.release()
            raise xmlrpc.Fault(404, "There're no such file in the system.")
        Lock.release()
        return result_list[newest_port],newest_port
    def xmlrpc_write(self, full_filename, version):
        Lock.acquire()
        parent, filename = get_parent_dir(full_filename)
        if parent not in file_dirs:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s." % parent)
        if filename not in file_dirs[parent]:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to read a file that doesn't exists: %s." % full_filename)
        result_list = get_versions_from_server(full_filename)
        if len(result_list) == 0:
            Lock.release()
            raise xmlrpc.Fault(404, "No live server.")
        sort_result = sorted(result_list)
        newest_port = sort_result[0]
        if result_list[newest_port] > version:
            Lock.release()
            raise xmlrpc.Fault(404, "Someone submits before you submit this file.")
        Lock.release()
        return newest_port
    def xmlrpc_delete(self, dir, cmd):
        if len(cmd) == 0:
            raise xmlrpc.Fault(403, "Forbidden Operation.")
        if len(subserver_port) == 0:
            raise xmlrpc.Fault(404, "No live server.")
        if cmd[0] == '/':
            full_filename = cmd
        else:
            full_filename = dir + cmd
        Lock.acquire()
        print("%sdel %s" % (dir, cmd))
        parent, filename = get_parent_dir(full_filename)
        if parent not in file_dirs:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to enter a directory that doesn't exist: %s." % parent)
        if filename not in file_dirs[parent]:
            Lock.release()
            raise xmlrpc.Fault(404, "You're trying to read a file that doesn't exists: %s." % full_filename)
        deleting_file.append(full_filename)
        file_dirs[parent].remove(filename)
        update_dirfile()
        Lock.release()
        return 0
    def xmlrpc_fault(self):
        raise xmlrpc.Fault(1, "Unknown error.")

if __name__ == '__main__':
    from twisted.internet import reactor
    if os.path.exists(dir_filename):
        read_file = open(dir_filename,'r')
        data = read_file.read()
        read_file.close()
        file_dirs = json.loads(data)
    else:
        json_data = json.dumps(file_dirs)
        new_file = open(dir_filename, 'w')
        new_file.write(json_data)
        new_file.close()

    # backup thread
    backup_thread = dir_backup()
    backup_thread.start()

    # timeout thread
    to_thread = timeout_thread()
    to_thread.start()

    # delete thread
    del_thread = delete_thread()
    del_thread.start()

    # rpc server
    rpc_srv = filedirsystem()
    reactor.listenTCP(9999, server.Site(rpc_srv))
    print("Main server begins to run.")
    reactor.run()
