#!/usr/bin/python

# Copyright (c) 2013 Jonathan Wedell <jonwedell@gmail.com> (author)
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import os
import sys
import time
import errno
import StringIO
import optparse
import subprocess
import threading
from multiprocessing import cpu_count
from fuse import FUSE, FuseOSError, Operations


class backgroundTask():

    def __init__(self, host):
        self.host = host

    def run(self):
        while True:
            job = None
            with self.host.slock:
                if self.host.dead:
                    os._exit(0)
                if len(self.host.process_list) > 0:
                    job = self.host.process_list.pop(0)

            if job is not None:
                job.encode()
            else:
                time.sleep(.1)
                to_remove = []
                now = time.time()
                with self.host.slock:
                    for key in self.host.known_files:
                        if self.host.known_files[key].last_read + options.cachetime < now:
                            to_remove.append(key)
                    for item in to_remove:
                        if options.debug: print "uncaching(",item,")"
                        del self.host.known_files[item]

class conversionObj():

    def __init__(self, abspath):
        """ Initialize! """
        self.path = abspath
        self.last_read = time.time()
        self.lock = threading.Lock()
        self.status = 0

        # Transcoded info
        self.encoded = 0
        self.size = os.path.getsize(abspath)
        self.data = StringIO.StringIO()

    def encode(self):
        """ Start the transcode process, release the lock once in a while to allow other threads to read from the transcoded file before the transcoding completes."""

        # We are changing ourself, don't allow concurrent access
        with self.lock:
            # Don't encode ourselves twice
            if self.status != 0:
                return
            self.status == 1

        # First get the tags
        tag_cmd = subprocess.Popen(['metaflac', '--export-tags-to', '-', self.path],stdout=subprocess.PIPE)
        tags = {}
        for sp in map(lambda x:x.partition("="), tag_cmd.stdout.read().split("\n")):
            if sp[0]:
                tags[sp[0]] = sp[2]
        tag_cmd.wait()

        # Then get the decoded FLAC stream
        flac = subprocess.Popen(['flac', '-c', '-d', self.path],stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Then set up the lame arguments and call lame
        lamecmd = ['lame','-q0', '-V', options.v, '--vbr-new', '--ignore-tag-errors', '--add-id3v2', '--pad-id3v2', '--ignore-tag-errors', '--ta', tags.get('artist','?'), '--tt', tags.get('title','?'), '--tl', tags.get('album','?'), '--tg', tags.get('genre','?'), '--tn', tags.get('tracknumber','?'), '--ty', tags.get('date','?'), '-', '-']
        lame = subprocess.Popen(lamecmd,stdout=subprocess.PIPE,stdin=flac.stdout,stderr=subprocess.PIPE)

        # Read the transcoded data in 150000 bit chunks
        while True:
            buff_read = lame.stdout.read(150000)
            # Lock before changing ourself
            with self.lock:
                self.data.seek(0,2)
                self.data.write(buff_read)
                self.encoded += len(buff_read)
                if len(buff_read) < 150000:
                    self.size = self.encoded
                    self.status = 2
                    break

        # Wait for the processes to finish (in practice they are already done)
        flac.wait()
        lame.wait()


    def read(self,offset,length):
        """ Read data from the file. Will return as soon as enough of the file is transcoded to meet the request."""
        data = None
        while data is None:
            with self.lock:
                if self.encoded > (offset + length) or self.status == 2:
                    self.data.seek(offset)
                    data = self.data.read(length)
            if data is None:
                time.sleep(.1)

        self.last_read = time.time()
        return data


class vbrConvert(Operations):

    def __init__(self, root):
        self.root = root
        self.dead = False
        self.slock = threading.Lock()
        self.known_files = {}
        self.process_list = []

        # Multi-thread our work
        self.workers = []
        for x in range(options.threads):
            worker = backgroundTask(self)
            worker.daemon = True
            self.workers.append(threading.Thread(target=worker.run))
        for thread in self.workers: thread.start()

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def _absolutePath(self, partial):

        if os.path.isfile(self._full_path(partial)):
            return os.path.abspath(self._full_path(partial))
        else:
            return os.path.abspath(self._full_path(partial)).replace('.mp3','.flac').replace('.MP3','.FLAC')

    def convFile(self, path):
        # We don't need to convert a given file twice
        if path in self.known_files:
            return

        # Don't do anything with existing files
        if os.path.isfile(self._full_path(path)) or os.path.isdir(self._full_path(path)):
            return

        # Get the actual path
        abspath = self._absolutePath(path)

        # Start the transcoding of ourself
        conv_obj = conversionObj(self._absolutePath(path))
        with self.slock:
            self.process_list.append(conv_obj)
            self.known_files[path] = conv_obj

        # Start prefetching
        if options.prefetch:
           next_files = os.listdir(os.path.dirname(abspath))
           ind = next_files.index(os.path.basename(abspath)) + 1
           for x in range(x,len(next_files)):
                if

        return


    # Filesystem methods
    # ==================

    def access(self, path, mode):
        if options.debug: print "access(",path,",",mode,")"
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        if options.debug: print "chmod(",path,",",mode,")"
        raise OSError(38,"Not implemented.")

    def chown(self, path, uid, gid):
        if options.debug: print "chown(",path,",",uid,",",gid,")"
        raise OSError(38,"Not implemented.")

    def getattr(self, path, fh=None):
        if options.debug: print "getattr(",path,",",fh,")"

        # Make sure they can't grab the actual FLACs
        if path[-5:] == ".flac" or path[-5:] == ".FLAC":
            raise OSError(2,"No such file or directory.")

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
            with self.known_files[path].lock:
                res['st_size'] = self.known_files[path].size

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
        raise OSError(38,"Not implemented.")

    def rmdir(self, path):
        if options.debug: print "rmdir(",path,")"
        raise OSError(38,"Not implemented.")

    def mkdir(self, path, mode):
        if options.debug: print "mkdir(",path,",",mode,")"
        raise OSError(38,"Not implemented.")

    def statfs(self, path):
        if options.debug: print "statfs(",path,")"
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        if options.debug: print "unlink(",path,")"
        raise OSError(38,"Not implemented.")

    def symlink(self, target, name):
        if options.debug: print "symlink(",target,",",name,")"
        raise OSError(38,"Not implemented.")

    def rename(self, old, new):
        if options.debug: print "rename(",old,",",new,")"
        raise OSError(38,"Not implemented.")

    def link(self, target, name):
        if options.debug: print "link(",taget,",",name,")"
        raise OSError(38,"Not implemented.")

    def utimens(self, path, times=None):
        if options.debug: print "utimens(",path,",",times,")"
        raise OSError(38,"Not implemented.")

    # File methods
    # ============

    def open(self, path, flags):
        if options.debug: print "open(",path,",",flags,")"

        if flags == 34817:
            raise OSError(38,"Not implemented.")

        full_path = self._absolutePath(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        if options.debug: print "create(",path,",",mode,",",fi,")"
        raise OSError(38,"Not implemented.")

    def read(self, path, length, offset, fh):
        if options.debug: print "read(",path,",",length,",",offset,",",fh,")"

        # Convert the file
        self.convFile(path)

        # Find out what to attach them to
        if path in self.known_files:

            return self.known_files[path].read(offset,length)
        else:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        if options.debug: print "write(",path,",[BUFF],",offset,",",fh,")"
        raise OSError(38,"Not implemented.")

    def truncate(self, path, length, fh=None):
        if options.debug: print "truncate(",path,",",length,",",fh,")"
        raise OSError(38,"Not implemented.")

    def flush(self, path, fh):
        if options.debug: print "flush(",path,",",fh,")"
        raise OSError(38,"Not implemented.")

    def release(self, path, fh):
        if options.debug: print "release(",path,",",fh,")"
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        if options.debug: print "fsync(",path,",",fdatasync,",",fh,")"
        raise OSError(38,"Not implemented.")


if __name__ == '__main__':

    # Specify some basic information about our command
    usage = "usage: %prog [options] flacdir mp3dir"
    parser = optparse.OptionParser(usage=usage,version="%prog .1",description="This program will present all FLACS as VBR mp3s. Like mp3fs but with VBR.")

    # Set up the option groups
    basic = optparse.OptionGroup(parser,"Basic options","If the default isn't good enough for you.")
    parser.add_option_group(basic)
    advanced = optparse.OptionGroup(parser,"Advanced options","You may want to use some of these.")
    parser.add_option_group(advanced)
    devel = optparse.OptionGroup(parser,"Developer options","You really shouldn't use these unless you know what you are doing.")
    parser.add_option_group(devel)

    # Specify the common arguments
    basic.add_option("--background", action="store_false", dest="foreground", default=True, help="Run this command in the background. (Currently broken.)")
    basic.add_option("--v", action="store", dest="v", type="choice", default="0", choices=(map(lambda x:str(x),range(0,10))), help="What V level do you want to transcode to? Default: %default.")
    basic.add_option("--noprefetch", action="store_false", dest="prefetch", default=True, help="Disable auto-transcoding of files that we expect to be read soon.")
    advanced.add_option("--threads", action="store", dest="threads", default=cpu_count()-1, type="int", help="How many threads should we use? This ideally should be set to one fewer than the number of cores you have available. Default: %default")
    advanced.add_option("--cache-time", action="store", dest="cachetime", default=60, type="int", help="How may seconds should we keep the transcoded MP3s in ram after they are last touched?")
    devel.add_option("--always-conv", action="store_true", dest="attrbconv", default=False, help="Will convert flac->mp3 even for things like 'ls'. Only needed if you want 'ls' to show the right file size, but doing so will be very slow. (You are forcing vbrfs to transcode a directory just to 'ls'.)")
    devel.add_option("--debug", action="store_true", dest="debug", default=False, help="Print every action sent to the filesystem to stdout. Implies --foreground.")

    # Options, parse 'em
    (options, args) = parser.parse_args()

    # We can only debug in the foreground.
    if options.debug:
        options.foreground = True

    # Check for invalid command line options
    if len(args) < 2:
        print "You must specify the flac directory and the mp3 directory."
        sys.exit(0)
    if len(args) > 2:
        print "Did you accidentally leave out an option? I don't accept three arguments."
        sys.exit(0)

    # Make the magic happen
    vbr = vbrConvert(args[0])
    FUSE(vbr, args[1], foreground=options.foreground)

    # Kill the workers when we exit
    with vbr.slock:
        vbr.dead = True
    sys.exit(0)

    a = conversionObj('/zdrive/NAS/music/Nick Drake/Five Leaves Left/01 - Time Has Told Me.flac')
    t = threading.Thread(target=a.encode)
    t.start()
    while True:
        with a.lock:
            if a.status == 2:
                break
            print a.encoded
        time.sleep(.1)
