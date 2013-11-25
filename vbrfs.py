#!/usr/bin/env python

import os
import sys
import errno
import StringIO
import optparse
import subprocess

from fuse import FUSE, FuseOSError, Operations


class v0Convert(Operations):

    def __init__(self, root):
        self.root = root
        self.known_files = {}

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def _absolutePath(self, partial):
        return os.path.abspath(self._full_path(partial)).replace('.mp3','.flac').replace('.MP3','.FLAC')

    def convFile(self, path):
        # We don't need to convert a given file twice
        if path in self.known_files:
            return

        # Get the actual path
        abspath = self._absolutePath(path)

        # Don't do anything for non-mp3 files
        if not ".flac" in abspath and not ".FLAC" in abspath:
            return

        # First get the tags
        tag_cmd = subprocess.Popen(['metaflac', '--export-tags-to', '-', abspath],stdout=subprocess.PIPE)
        tags = {}
        for sp in map(lambda x:x.partition("="), tag_cmd.stdout.read().split("\n")):
            if sp[0]:
                tags[sp[0]] = sp[2]
        tag_cmd.wait()

        # Then get the decoded FLAC stream
        flac = subprocess.Popen(['flac', '-c', '-d', abspath],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Then set up the lame arguments and call lame
        lamecmd = ['lame','-q0', '-V0', '--vbr-new', '--ignore-tag-errors', '--add-id3v2', '--pad-id3v2', '--ignore-tag-errors', '--ta', tags.get('artist','?'), '--tt', tags.get('title','?'), '--tl', tags.get('album','?'), '--tg', tags.get('genre','?'), '--tn', tags.get('tracknumber','?'), '--ty', tags.get('date','?'), '-', '-']
        lame = subprocess.Popen(lamecmd,stdout=subprocess.PIPE,stdin=flac.stdout,stderr=subprocess.PIPE)
        # Create a file-like object with the resulting mp3 stream
        self.known_files[path] = StringIO.StringIO(lame.stdout.read())
        # Wait for the processes to finish (in practice they are already done)
        flac.wait()
        lame.wait()


    # Filesystem methods
    # ==================

    def access(self, path, mode):
        if options.debug: print "access(",path,",",mode,")"
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        if options.debug: print "chmod(",path,",",mode,")"
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        if options.debug: print "chown(",path,",",uid,",",gid,")"
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        if options.debug: print "getattr(",path,",",fh,")"
        full_path = self._absolutePath(path)

        # Only convert files for getattr requests if option enabled
        if options.attrbconv:
            self.convFile(path)

        # Stat the file
        st = os.lstat(full_path)
        res =  dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

        # Update the size if we have a mp3 version of the file
        if path in self.known_files:
            self.known_files[path].seek(0,2)
            res['st_size'] = self.known_files[path].tell()

        # Return the results
        return res

    def readdir(self, path, fh):
        if options.debug: print "readdir(",path,",",fh,")"
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r.replace('.flac','.mp3').replace('.FLAC','.MP3')

    def readlink(self, path):
        if options.debug: print "readline(",path,")"
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        if options.debug: print "mknod(",path,",",mode,",",dev,")"
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        if options.debug: print "rmdir(",path,")"
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        if options.debug: print "mkdir(",path,",",mode,")"
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        if options.debug: print "statfs(",path,")"
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        if options.debug: print "unlink(",path,")"
        return os.unlink(self._full_path(path))

    def symlink(self, target, name):
        if options.debug: print "symlink(",target,",",name,")"
        return os.symlink(self._full_path(target), self._full_path(name))

    def rename(self, old, new):
        if options.debug: print "rename(",old,",",new,")"
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        if options.debug: print "link(",taget,",",name,")"
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        if options.debug: print "utimens(",path,",",times,")"
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        if options.debug: print "open(",path,",",flags,")"

        full_path = self._absolutePath(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        if options.debug: print "create(",path,",",mode,",",fi,")"
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        if options.debug: print "read(",path,",",length,",",offset,",",fh,")"

        # Convert the file
        self.convFile(path)

        # Find out what to attach them to
        if path in self.known_files:
            self.known_files[path].seek(offset)
            return self.known_files[path].read(length)
        else:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        if options.debug: print "write(",path,",",buf,",",offset,",",fh,")"
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        if options.debug: print "truncate(",path,",",length,",",fh,")"
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        if options.debug: print "flush(",path,",",fh,")"
        return os.fsync(fh)

    def release(self, path, fh):
        if options.debug: print "release(",path,",",fh,")"
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        if options.debug: print "fsync(",path,",",fdatasync,",",fh,")"
        return self.flush(path, fh)


if __name__ == '__main__':

    # Specify some basic information about our command
    usage = "usage: vbrfs [options] flacdir mp3dir"
    parser = optparse.OptionParser(usage=usage,version="%prog .1",description="This program will present all FLACS as VBR mp3s. Like mp3fs but with VBR.")

    devel = optparse.OptionGroup(parser,"Developer options","You really shouldn't use these unless you know what you are doing.")
    parser.add_option_group(devel)

    # Specify the common arguments
    parser.add_option("--attrbconv", action="store_true", dest="attrbconv", default=False, help="Will convert flac->mp3 even for things like ls. Not recomended.")
    parser.add_option("--foreground", action="store_true", dest="foreground", default=False, help="Run this command in the foreground.")
    devel.add_option("--debug", action="store_true", dest="debug", default=False, help="Print every action sent to the filesystem to stdout.")

    # Options, parse 'em
    (options, args) = parser.parse_args()

    if len(args) < 2:
        print "You must specify the flac directory and the mp3 directory."
        sys.exit(0)
    if len(args) > 2:
        print "Did you accidentally leave out an option? I don't accept three arguments."
        sys.exit(0)

    # Make the magic happen
    FUSE(v0Convert(args[0]), args[1], foreground=options.foreground)
