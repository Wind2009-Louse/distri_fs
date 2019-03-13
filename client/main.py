import xmlrpc.client
import hashlib
import os
import json

fileversion = {}
version_filename = "%s/dir.txt"%os.getcwd()

def dir_md5(dir):
    return hashlib.md5(bytes(dir,encoding='utf-8')).hexdigest()

def file_realname(dir):
    dir_name = "%s/cache"%(os.getcwd())
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    return "%s/cache/%s" % (os.getcwd(), dir_md5(dir))

def update_version():
    global fileversion
    write_json = json.dumps(fileversion)
    new_file = open(version_filename, 'w')
    new_file.write(write_json)
    new_file.close()

if __name__ == '__main__':
    # load fileversion
    if os.path.exists(version_filename):
        read_file = open(version_filename, 'r')
        data = read_file.read()
        read_file.close()
        file_dirs = json.loads(data)
    else:
        json_data = json.dumps(fileversion)
        new_file = open(version_filename, 'w')
        new_file.write(json_data)
        new_file.close()
    # initialize
    srv = xmlrpc.client.Server('http://localhost:9999/')
    current_dir = "/"
    # working
    while(True):
        command = input(current_dir + ">")
        if command == 'dir':
            try:
                result = srv.dir(current_dir)
                print(result)
            except Exception as err:
                print(format(err))
        elif command[0:5] == 'mkdir' and len(command) > 6:
            tomake_dir = command[6:]
            make_step = tomake_dir.split('/')
            if (tomake_dir[0] != '/'):
                into_dir = current_dir
            else:
                into_dir = '/'
            try:
                for newdir in make_step:
                    if newdir != "":
                        srv.mkdir(into_dir, newdir)
                        into_dir += (newdir+"/")
                print("Succeed!")
            except Exception as err:
                print(format(err))
        elif command[0:2] == 'cd':
            if (command == 'cd'):
                print(current_dir)
                continue
            try:
                new_dir = srv.cd(current_dir, command[3:])
                current_dir = new_dir
            except Exception as err:
                print(format(err))
        elif command[0:2] == 'rm' and len(command) > 3:
            confirm = input("Do you really want to remove? It will delete everything included.\nTo confirm, please input Y")
            if confirm != 'Y':
                continue
            try:
                new_dir = srv.rm(current_dir, command[3:])
                current_dir = new_dir
                print("Delete!")
            except Exception as err:
                print(format(err))
        elif command[0:3] == 'del' and len(command) > 4:
            confirm = input(
                "Do you really want to delete the file?\nTo confirm, please input Y")
            if confirm != 'Y':
                continue
            try:
                srv.delete(current_dir, command[4:])
                print("Delete!")
            except Exception as err:
                print(format(err))
        elif command[0:2] == 'mf' and len(command) > 3:
            tomake = command[3:]
            try:
                srv.create(current_dir, tomake)
                print("Succeed!")
            except Exception as err:
                print(format(err))
        elif command[0:4] == 'open' and len(command) > 5:
            toopen = command[5:]
            try:
                if toopen[0] == '/':
                    real_file = toopen
                else:
                    real_file = current_dir + toopen
                real_file_hash = file_realname(real_file)
                version, srv_port = srv.read(current_dir, real_file)
                # read file from server
                if real_file not in fileversion\
                    or fileversion[real_file] < version:
                    f_srv = xmlrpc.client.Server('http://localhost:%d/'%srv_port)
                    f_version, data = f_srv.readfile(real_file)
                    if (f_version != version):
                        print("Illegal version!")
                        continue
                    file = open(real_file_hash,'wb')
                    file.write(data.data) # data: Binary
                    file.close()
                    fileversion[real_file] = version
                    update_version()
                file = open(real_file_hash,'rb')
                before_data = file.read()
                file.close()
                os.system("notepad.exe %s"%real_file_hash)
                file = open(real_file_hash, 'rb')
                after_data = file.read()
                file.close()
                if before_data != after_data:
                    srv_port = srv.write(real_file, version)
                    f_srv = xmlrpc.client.Server('http://localhost:%d/' % srv_port)
                    f_srv.writefile(version, real_file, after_data)
            except Exception as err:
                print(format(err))
        elif command == "Exit":
            print("Bye!")
            exit(0)
        else:
            print("Illegal command!")
