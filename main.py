"""Hey, i'm pet project """

import os
import argparse
import sys
import json
from stat import S_IFDIR, S_IFLNK
from time import time
import errno
import pprint
from uuid import uuid4
import redis

from fuse import FUSE, FuseOSError, Operations, fuse_get_context


class Passthrough(Operations):
    # pylint: disable=W0221
    # pylint: disable=R0904
    # pylint: disable=W0707
    # pylint: disable=R0913:
    """default fuse class"""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def access(self, path, mode):
        print("access", path, mode)
        return 0

    def chmod(self, path, mode):
        print("chmod", path, mode)
        print("len path", len(path))
        children = os.path.basename(path)
        if len(children) > 256:
            raise FuseOSError(errno.ENAMETOOLONG)

        now = time()

        inode = str(self.redis_client.hget(path, "inode"), encoding='utf-8')
        attr = json.loads(self.redis_client.hget(inode, "attr"))
        print("Got", attr['st_mode'])
        attr['st_atime'] = now
        attr['st_ctime'] = now
        attr['st_mtime'] = now
        attr['st_mode'] = mode
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        return 0

    def chown(self, path, uid, gid):
        print("chown", path, uid, gid)
        now = time()
        # itype = str(self.redis_client.hget(path, "type"), encoding='utf-8')
        # if itype == "hardlink":
        #     print("hehey")
        #     parent = self.redis_client.hget(path, "parent")
        #     filepath = parent
        # else:
        #     filepath = path
        inode = self.redis_client.hget(path, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))
        print("Got", attr['st_mode'])
        attr['st_atime'] = now
        attr['st_ctime'] = now
        attr['st_mtime'] = now
        if gid != -1:
            attr['st_gid'] = gid
        if uid != -1:
            attr['st_uid'] = uid
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        return 0

    def getattr(self, path, fh=None):
        print("!getattr " + path)
        print("key = " + path)
        children = os.path.basename(path)
        print("children ", children)
        if len(children) >= 256:
            raise FuseOSError(errno.ENAMETOOLONG)

        try:
            inode = str(self.redis_client.hget(path, "inode"), encoding='utf-8')
            #     itype = str(self.redis_client.hget(path, "type"), encoding='utf-8')
            #     if itype == "hardlink":
            #         print("hehey")
            #         parent = self.redis_client.hget(path, "parent")
            #         attr = json.loads(self.redis_client.hget(inode, "attr"))
            #     else:
            attr = json.loads(self.redis_client.hget(inode, "attr"))
            pprint.pprint(attr)

        except TypeError:
            print("not found")
            raise FuseOSError(errno.ENOENT)
        return attr

    def getxattr(self, path, name, position=0):
        print("getxattr {} name {}".format(path, name))
        return b"dds"

    def setxattr(self, path, name, value, options, position=0):
        print("getxattr " + path)
        print("key = " + path)
        return 0

    def removexattr(self, path, name):
        print("removexattr " + path)
        now = time()
        inode = self.redis_client.hget(path, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))

        attr['st_ctime'] = now

        self.redis_client.hset(inode, "attr", json.dumps(attr))
        return 0

    def readdir(self, path, fh):
        print("readdir")

        dirents = ['.', '..']
        print(path)
        print(self.redis_client.lrange(path + ":children", 0, -1))
        for children in self.redis_client.lrange(path + ":children", 0, -1):
            dirents.append(str(children, encoding='utf-8'))
        print(dirents)

        for cdir in dirents:
            yield cdir

    def readlink(self, path):
        print("readlink", path)
        return str(self.redis_client.hget(path, "parent"), encoding='utf-8')

    def mknod(self, path, mode, dev):
        print("mknod", path, mode, dev)
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        uid, gid, _ = fuse_get_context()
        print(mode)
        now = time()
        icnt = int(int(self.redis_client.get('icnt'))) + 1

        attr = dict(
            st_atime=now,
            st_ctime=now,
            st_gid=gid,
            st_mode=mode,
            st_mtime=now,
            st_nlink=1,
            st_size=0,
            st_uid=uid,
            st_rdev=dev,
            st_ino=icnt,
        )
        inode = uuid_gen()
        self.redis_client.set('icnt', icnt)
        self.redis_client.hset(path, "type", "mknod")
        self.redis_client.hset(path, "inode", inode)
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        self.redis_client.hset(path, "parent", path)
        self.redis_client.lpush(parent + ":children", children)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))
        return 0

    def rmdir(self, path):
        print("rmdir")
        now = time()
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        all_keys = list(self.redis_client.hgetall(path).keys())
        inode = self.redis_client.hget(path, "inode")
        print(all_keys)

        if len(self.redis_client.lrange(path + ":children", 0, -1)) > 0:
            raise FuseOSError(errno.ENOTEMPTY)

        self.redis_client.hdel(path, *all_keys)
        self.redis_client.delete(inode)
        self.redis_client.lrem(parent + ":children", 0, children)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))
        return 0

    def mkdir(self, path, mode):
        print("mkdir")
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        now = time()
        uid, gid, _ = fuse_get_context()
        icnt = int(self.redis_client.get('icnt')) + 1
        attr = dict(
            st_atime=now,
            st_ctime=now,
            st_gid=gid,
            st_uid=uid,
            st_size=0,
            st_mode=(S_IFDIR | mode),
            st_mtime=now,
            st_nlink=2,
            st_ino=icnt
        )
        print(attr)
        inode = uuid_gen()
        self.redis_client.set('icnt', icnt)
        self.redis_client.hset(path, "type", "dir")
        self.redis_client.hset(path, "inode", inode)
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        self.redis_client.hset(path, "parent", path)
        self.redis_client.lpush(parent + ":children", children)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))

        return 0

    def statfs(self, path):
        print("statfs")
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        now = time()
        inode = self.redis_client.hget(path, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))
        if attr['st_nlink'] > 1:
            print("not last link to inode")
            attr['st_nlink'] -= 1
            attr['st_ctime'] = now
            attr['st_mtime'] = now
            all_keys = list(self.redis_client.hgetall(path).keys())
            self.redis_client.hdel(path, *all_keys)
            self.redis_client.lrem(parent + ":children", 0, children)
            self.redis_client.hset(inode, "attr", json.dumps(attr))

        else:
            all_keys = list(self.redis_client.hgetall(path).keys())
            self.redis_client.hdel(path, *all_keys)
            self.redis_client.lrem(parent + ":children", 0, children)
            self.redis_client.hset(inode, "attr", json.dumps(attr))
            self.redis_client.delete(inode)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))

        print("unlink")
        return 0

    def symlink(self, path, target):
        print("symlink", path, target)
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        uid, gid, _ = fuse_get_context()
        now = time()
        icnt = int(self.redis_client.get('icnt')) + 1
        attr = dict(
            st_atime=now,
            st_ctime=now,
            st_gid=gid,
            st_mode=(S_IFLNK | 644),
            st_mtime=now,
            st_nlink=1,
            st_size=0,
            st_uid=uid,
            st_ino=icnt,
        )
        self.redis_client.set('icnt', icnt)
        inode = uuid_gen()
        self.redis_client.hset(path, "type", "symlink")
        self.redis_client.hset(path, "dst", target)
        self.redis_client.hset(path, "inode", inode)
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        self.redis_client.hset(path, "parent", target)
        self.redis_client.lpush(parent + ":children", children)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))
        return 0

    def rename(self, old, new):
        print("rename", old, new)
        now = time()
        old_parent = os.path.dirname(old)
        old_children = os.path.basename(old)
        new_parent = os.path.dirname(new)
        new_children = os.path.basename(new)

        if self.redis_client.hget(new, "type"):
            if self.redis_client.hget(new, "type") == b"dir":
                if len(self.redis_client.lrange(new + ":children", 0, -1)) > 0:
                    raise FuseOSError(errno.ENOTEMPTY)

                # raise FuseOSError(errno.EEXIST)

        self.redis_client.lrem(old_parent + ":children", 0, old_children)
        self.redis_client.rename(old, new)
        self.redis_client.lpush(new_parent + ":children", new_children)
        inode = self.redis_client.hget(new, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))
        attr['st_ctime'] = now
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        return 0

    def link(self, path, target):
        print("link", target, path)
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        parent_hardlink = False
        now = time()
        inode = self.redis_client.hget(target, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))
        # try:
        #     itype = str(self.redis_client.hget(target, "type"), encoding='utf-8')
        #     if itype == "hardlink":
        #         print("hehey")
        #         parent_hardlink = self.redis_client.hget(target, "parent")
        #         attr = json.loads(self.redis_client.hget(parent_hardlink, "attr"))
        #     else:
        #         attr = json.loads(self.redis_client.hget(target, "attr"))
        #
        # except TypeError:
        #     print("not found")
        #     raise FuseOSError(errno.ENOENT)

        if attr['st_nlink'] == 1:
            print("no links")
            links = [path]
        elif parent_hardlink:
            links = json.loads(self.redis_client.hget(inode, "links"))
            links.append(path)
        else:
            links = json.loads(self.redis_client.hget(inode, "links"))
            links.append(path)

        attr['st_nlink'] += 1
        attr['st_ctime'] = now
        attr['st_mtime'] = now

        self.redis_client.hset(inode, "links", json.dumps(links))
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        self.redis_client.lpush(parent + ":children", children)
        self.redis_client.hset(path, "type", "hardlink")
        self.redis_client.hset(path, "inode", inode)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))
        print("link created")

        return 0

    def utimens(self, path, times=None):
        print("utimems", path, times)
        atime, mtime = times

        print("atime {}, mtime {}".format(atime, mtime))

        inode = self.redis_client.hget(path, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))
        attr['st_atime'] = atime
        attr['st_mtime'] = mtime
        self.redis_client.hset(inode, "attr", json.dumps(attr))
        return 0

    def open(self, path, flags):
        print("open", path, flags)
        return 0

    def create(self, path, mode, fi=None):
        print("create", path, mode, fi)
        print(path)
        parent = os.path.dirname(path)
        children = os.path.basename(path)
        uid, gid, _ = fuse_get_context()
        print(mode)
        now = time()
        icnt = int(self.redis_client.get('icnt')) + 1
        attr = dict(
            st_atime=now,
            st_ctime=now,
            st_gid=gid,
            st_mode=mode,
            st_mtime=now,
            st_nlink=1,
            st_size=0,
            st_uid=uid,
            st_ino=icnt,
        )

        self.redis_client.set('icnt', icnt)

        inode = uuid_gen()
        self.redis_client.hset(path, "type", "file")
        self.redis_client.hset(path, "inode", inode)

        self.redis_client.hset(inode, "attr", json.dumps(attr))
        self.redis_client.hset(path, "parent", path)
        self.redis_client.lpush(parent + ":children", children)

        parent_inode = self.redis_client.hget(parent, "inode")
        parent_attr = json.loads(self.redis_client.hget(parent_inode, "attr"))
        parent_attr['st_ctime'] = now
        parent_attr['st_mtime'] = now
        self.redis_client.hset(parent_inode, "attr", json.dumps(parent_attr))
        return 0

    def read(self, path, length, offset, fh):
        print("read")
        print("Length", length)
        print("Offset", offset)

        inode = self.redis_client.hget(path, "inode")
        # if itype == "hardlink":
        #     print("hehey")
        #     parent = self.redis_client.hget(path, "parent")
        #     filepath = parent
        # else:
        #     filepath = path

        payload = self.redis_client.hget(inode, "payload")
        if payload is None:
            payload = ''
        reqested_payload = payload[offset:(offset + length)]
        print("got from redis:", len(payload))
        print("Got:", len(reqested_payload))
        return reqested_payload

    def write(self, path, buf, offset, fh):
        print("write")
        now = time()
        inode = self.redis_client.hget(path, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))

        attr['st_atime'] = now
        attr['st_mtime'] = now

        print(attr)
        if offset > 0:
            payload = self.redis_client.hget(inode, "payload") + buf
            print("write next chunk", len(buf))
        else:
            payload = buf
            print("got first chunk", len(buf))

        self.redis_client.hset(inode, "payload", payload)

        attr['st_size'] = len(payload)

        self.redis_client.hset(inode, "attr", json.dumps(attr))
        return len(buf)

    def truncate(self, path, length, fh=None):
        print("truncate", path, length, fh)
        now = time()
        inode = self.redis_client.hget(path, "inode")
        attr = json.loads(self.redis_client.hget(inode, "attr"))

        attr['st_atime'] = now
        attr['st_mtime'] = now
        attr['st_ctime'] = now

        payload = self.redis_client.hget(inode, "payload")
        # CHECK AND FIX THAT!!
        if length == 0:
            attr['st_size'] = 0
            self.redis_client.hset(inode, "payload", "")

        elif not payload:
            print("empty file")
            payload = b"".ljust(length, b'0')
            self.redis_client.hset(inode, "payload", payload)
            attr['st_size'] = length

        elif len(payload) < length:
            print("not empty file")

            attr['st_size'] = length
            payload.ljust(length - len(payload), b'0')
            self.redis_client.hset(inode, "payload", payload)

        else:
            attr['st_size'] = length

            self.redis_client.hset(inode, "payload", payload[:length])

        self.redis_client.hset(inode, "attr", json.dumps(attr))

        return 0

    def flush(self, path, fh):
        print("flush", path, fh)
        print(type(fh))
        return 0

    def release(self, path, fh):
        print("release", path, fh)
        return 0

    def fsync(self, path, fdatasync, fh):
        print("fsync", path, fdatasync, fh)
        return self.flush(path, fh)


def uuid_gen():
    """return string with uuid"""
    return str(uuid4())


def main(mountpoint, daemon):
    """main function"""
    redis_client = redis.Redis(host='localhost', port=6379, db=0)

    # check our fs has root, if not - create it!
    if not redis_client.hget("/", "type"):
        print("creating /")
        now = time()
        attr = dict(
            st_atime=now,
            st_ctime=now,
            st_gid=0,
            st_mode=16895,
            st_mtime=now,
            st_nlink=2,
            st_size=0,
            st_uid=0,
            st_ino=1,
        )
        inode = uuid_gen()
        redis_client.hset("/", "type", "dir")
        redis_client.hset("/", "inode", inode)
        redis_client.hset("/", "parent", "none")
        redis_client.set("icnt", 1)

        redis_client.hset(inode, "attr", json.dumps(attr))

    FUSE(Passthrough(redis_client), mountpoint, nothreads=True, debug=False, foreground=daemon,
         **{'allow_other': True,
            'attr_timeout': 0,
            'use_ino': True,
            'default_permissions': True})


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("mounpoint", help="mountpoint")
    PARSER.add_argument("--foreground", help="run programm in foreground mode", action="store_true")
    ARGS = PARSER.parse_args()

    main(sys.argv[1], ARGS.foreground)
