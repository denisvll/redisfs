import os
import io
import sys
import redis
import json
import errno
from time import time


from fuse import FUSE, FuseOSError, Operations

class Passthrough(Operations):
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def access(self, path, mode):
        print("access")
        return 0

    def chmod(self, path, mode):
        print("chmod")
        return 0

    def chown(self, path, uid, gid):
        print("chown")
        return 0

    def getattr(self, path, fh=None):
        print("getattr" + path)
        print("key = " + path)
        try:
            attr = json.loads(self.redis_client.hget(path,"attr"))
        except TypeError:
            print("not found")
            raise FuseOSError(errno.ENOENT)

        return attr

    def readdir(self, path, fh):
        print("readdir")

        dirents = ['.', '..']
        print(path)
        print(self.redis_client.lrange("/:children", 0, -1))
        for children in self.redis_client.lrange("/:children", 0, -1):
            dirents.append(str(children, encoding='utf-8'))
        print(dirents)

        for r in dirents:
            yield r

    def readlink(self, path):
        print("readlink")
        return 0

    def mknod(self, path, mode, dev):
        print("mknod")
        return 0

    def rmdir(self, path):
        print("rmdir")
        return 0

    def mkdir(self, path, mode):
        print("mkdir")
        return 0

    def statfs(self, path):
        print("statfs")
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        print("unlink")
        return 0

    def symlink(self, name, target):
        print("symlink")
        return 0

    def rename(self, old, new):
        print("rename")
        return 0

    def link(self, target, name):
        print("link")
        return 0

    def utimens(self, path, times=None):
        print("utimems")
        return 0



    def open(self, path, flags):
        print("open")
        return 0

    def create(self, path, mode, fi=None):
        print("create")
        print (path)
        print(mode)
        now = time()
        attr = dict(
            st_atime = now,
            st_ctime = now,
            st_gid = 1000,
            st_mode = 33188,
            st_mtime = now,
            st_nlink = 1,
            st_size = 0,
            st_uid = 1000
        )

        self.redis_client.hset(path,"type","file")
        self.redis_client.hset(path,"attr",json.dumps(attr))
        self.redis_client.hset(path,"parent","/")
        self.redis_client.lpush("/:children",path[1:])
        return 0

    def read(self, path, length, offset, fh):
        print("read")
        print(path)
        print(length)
        print(offset)
        print(fh)

        return self.redis_client.get("file")

    def write(self, path, buf, offset, fh):
        print("write")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        print("truncate")
        return 0

    def flush(self, path, fh):
        print("flush")
        print(type(fh))
        return 0

    def release(self, path, fh):
        print("release")
        return 0

    def fsync(self, path, fdatasync, fh):
        print("fsync")
        return self.flush(path, fh)


def main(mountpoint):
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    FUSE(Passthrough(redis_client), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
