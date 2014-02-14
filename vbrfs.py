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
import stat
import errno
import logging
import StringIO
import optparse
import threading
import subprocess
from errno import *

from multiprocessing import cpu_count
from fuse import FUSE, FuseOSError, Operations

class backgroundTask():

    def __init__(self, host, expiration_checker=False):
        self.host = host
        self.check_expiration = expiration_checker

    def run(self):

        while True:
            job = None
            with self.host.slock:
                if len(self.host.process_list) > 0:
                    job = self.host.process_list.pop(0)

            if job is not None:
                logger.debug("encoding( " + unicode(job.path) + " )")
                size = job.encode()
                if size is not None:
                    with self.host.sizelock:
                        with job.lock:
                            self.host.file_sizes[job.path] = size
                            logger.debug("sized( " + unicode(job.path) + " , " + unicode(self.host.file_sizes[job.path]) + " )")
            else:
                time.sleep(.1)

            # If we are assinged garbage checker duties
            if self.check_expiration:

                to_remove = []
                now = time.time()

                # Get the list of current conv_objects
                with self.host.slock:
                    keys = list(self.host.known_files)

                # Get all of the known files
                for key in keys:
                    conv_obj = None
                    with self.host.slock:
                        if key in self.host.known_files:
                            conv_obj = self.host.known_files[key]

                    # The conv_obj was already removed by another thread
                    if conv_obj is None:
                        continue

                    with conv_obj.lock:
                        # Object removed somewhere else
                        if conv_obj.status == -1:
                            to_remove.append(key)
                        # Object passed timeout
                        if conv_obj.status == 2 and conv_obj.last_read + options.cachetime < now:
                            to_remove.append(key)

                # Remove the deleted conv_objects from the file hash
                with self.host.slock:
                    for item in to_remove:
                        logger.debug("uncaching( " + unicode(item) + " )")
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

        # Read the transcoded data in 16kb chunks
        while True:
            buff_read = lame.stdout.read(131072)
            # Lock before changing ourself
            with self.lock:

                # If we aren't needed anymore just quit
                #if self.status == -1:
                #    lame.terminate()
                #    flac.terminate()
                #    return

                # Add the next part of the transcode onto our buffer
                self.data.seek(0,2)
                self.data.write(buff_read)
                self.encoded += len(buff_read)
                if len(buff_read) < 131072:
                    self.size = self.encoded
                    self.status = 2
                    break

        # Wait for the processes to finish (in practice they are already done)
        flac.wait()
        lame.wait()

        # Return our encoded size
        return self.size

    def read(self,offset,length):
        """ Read data from the file. Will return as soon as enough of the file is transcoded to meet the request."""

        # Don't allow reads from removed objects
        with self.lock:
            if self.status == -1:
                return None

        data = None
        while data is None:
            # We are waiting for a read even if we aren't reading
            self.last_read = time.time()

            with self.lock:
                if self.status == -1:
                    return None
                if self.encoded > (offset + length) or self.status == 2:
                    self.data.seek(offset)
                    data = self.data.read(length)

            if data is None: time.sleep(.1)

        return data


class vbrConvert(Operations):

    def __init__(self, root):
        self.root = root
        self.slock = threading.Lock()
        self.known_files = {}
        self.process_list = []

        # Keep track of converted file sizes forever and ever
        self.file_sizes = {}
        self.sizelock = threading.Lock()

        # Multi-thread our work
        self.workers = []
        for x in range(options.threads):
            worker = backgroundTask(self)
            if x == 0: worker.check_expiration = True
            the_thread = threading.Thread(target=worker.run)
            the_thread.daemon = True
            self.workers.append(the_thread)
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

    def convFile(self, path, getnext=True):
        # We don't need to convert a given file twice

        if getnext: logger.debug("convFile( " + unicode(path) + " , " + unicode(getnext) + " )")

        with self.slock:
            if path in self.known_files:
                return

        # Don't do anything with existing files
        if os.path.isfile(self._full_path(path)) or os.path.isdir(self._full_path(path)):
            # But do store their size
            with self.sizelock:
                self.file_sizes[self._absolutePath(path)] = os.path.getsize(self._absolutePath(path))
                logger.debug("sized( " + unicode(self._absolutePath(path)) + " , " + unicode(self.file_sizes[self._absolutePath(path)]) + " )")
            return

        # Get the actual path
        abspath = self._absolutePath(path)

        # Start the transcoding of ourself
        conv_obj = conversionObj(self._absolutePath(path))
        with self.slock:
            # Put actual requests in front of prefetch requests
            if getnext:
                self.process_list.insert(0,conv_obj)
            else:
                self.process_list.append(conv_obj)

            self.known_files[path] = conv_obj

        # Don't recursively prefetch, that way lies madness
        if not getnext:
            return conv_obj

        # Start prefetching
        if options.prefetch:
            next_files = os.listdir(os.path.dirname(abspath))
            ind = next_files.index(os.path.basename(abspath)) + 1
            for x in range(ind,len(next_files)):
                the_path = os.path.join(os.path.dirname(path),next_files[x]).replace('.flac','.mp3').replace('.FLAC','.MP3')
                logger.debug("prefetch( " + unicode(the_path) + " )")
                self.convFile(the_path,getnext=False)

        return conv_obj


    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        logger.debug("getattr( " + unicode(path) + " , " + unicode(fh) + " )")

        # Make sure they can't grab the actual FLACs
        if path[-5:] == ".flac" or path[-5:] == ".FLAC":
            path = path.replace(".flac",".mp3").replace(".FLAC",".MP3")
            # TODO: This is risky
            #raise OSError(2,"No such file or directory.")

        full_path = self._absolutePath(path)

        # Check if we already know the file size
        with self.sizelock:
            size = self.file_sizes.get(self._absolutePath(path),False)

        # Only convert files for getattr requests if option enabled
        if options.attrbconv and size is False and not os.path.islink(path):
            self.convFile(path)
            while size is False:
                with self.sizelock:
                    size = self.file_sizes.get(self._absolutePath(path),False)
                if size is False:
                    time.sleep(.1)

        # Stat the file
        st = os.lstat(full_path)
        res =  dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

        # This makes all files appear to have read-only permissions
        if options.modperms:
            # We want to show all files as read-only only
            res['st_mode'] =  stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IWUSR
            # Add the right attributes
            if os.path.islink(full_path):
                res['st_mode'] = res['st_mode'] | stat.S_IFLNK
            elif os.path.isfile(full_path):
                res['st_mode'] = res['st_mode'] | stat.S_IFREG
            elif os.path.isdir(full_path):
                res['st_mode'] = res['st_mode'] | stat.S_IFDIR | stat.S_IXUSR
            else:
                raise OSError(2,"No such file or directory.")

        # Update the size if we have a mp3 version of the file
        if size is not False and not os.path.islink(full_path):
            res['st_size'] = size

        # Return the results
        return res

    def readdir(self, path, fh):
        logger.debug("readdir( " + unicode(path) + " , " + unicode(fh) + " )")
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r.replace('.flac','.mp3').replace('.FLAC','.MP3')

    def readlink(self, path):
        logger.debug("readlink( " + unicode(path) + " )")

        path = self._absolutePath(path)
        pathname = os.readlink(path)
        #pathname = self._absolutePath(pathname)
        # TODO: needs more work

        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def statfs(self, path):
        logger.debug("statfs( " + unicode(path) + " )")
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))


    # File methods
    # ============

    def open(self, path, flags):
        logger.debug("open( " + unicode(path) + " , " + unicode(flags) + " )")

        # Convert the file
        self.convFile(path)

        full_path = self._absolutePath(path)

        # Always return made up fd for our transcodes
        if os.path.isfile(self._full_path(path)) or os.path.isdir(self._full_path(path)):
            return os.open(full_path, flags)
        else:
            return 1

    def read(self, path, length, offset, fh):
        logger.debug("read( " + unicode(path) + " , " + unicode(length) + " , " + unicode(offset) + " , " + unicode(fh) + " )")


        # Get the conv_obj to read the result from
        with self.slock:
            conv_obj = self.known_files.get(path,None)
        # Read an actual file off of disk
        if conv_obj is None:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)
        else:
            return conv_obj.read(offset,length)

    def flush(self, path, fh):
        logger.debug("flush( " + unicode(path) + " , " + unicode(fh) + " )")
        return 0

    def release(self, path, fh):
        logger.debug("release( " + unicode(path) + " , " + unicode(fh) + " )")

        # Remove the transcode on file close if they specified the read once argument
        if not options.keep_on_release:

            with self.slock:
                conv_obj = self.known_files.get(path,None)
            if conv_obj is not None:
                with conv_obj.lock:
                    conv_obj.status = -1

        # Only close the file if it is a real file
        if os.path.isfile(self._full_path(path)) or os.path.isdir(self._full_path(path)):
            os.close(fh)


if __name__ == '__main__':

    # Specify some basic information about our command
    usage = "usage: %prog [options] flacdir mp3dir"
    parser = optparse.OptionParser(usage=usage,version="%prog 1",description="This program will present all FLACS as VBR mp3s. Like mp3fs but with VBR. It will add basic idv2 tags but it will not transfer all tags.")

    # Set up the option groups
    basic = optparse.OptionGroup(parser,"Basic options","If the default isn't good enough for you.")
    parser.add_option_group(basic)
    advanced = optparse.OptionGroup(parser,"Advanced options","You may want to use some of these.")
    parser.add_option_group(advanced)
    devel = optparse.OptionGroup(parser,"Developer options","You really shouldn't use these unless you know what you are doing.")
    parser.add_option_group(devel)

    # Specify the common arguments
    basic.add_option("-v","--V", action="store", dest="v", type="choice", default="0", choices=(map(lambda x:str(x),range(0,10))), help="What V level do you want to transcode to? Default: %default.")
    basic.add_option("--minimal", action="store_true", dest="minimal", default=False, help="Automatically chooses options to allow this to work on low power machines. Implies --noprefetch, --threads 1, and --cachetime 30.")
    advanced.add_option("--multiread", action="store_true", dest="keep_on_release", default=False, help="Keep a transcode in memory after it has been read. (Until the cache timeout.) Useful if you will read the same file multiple times in succession.")
    advanced.add_option("--foreground", action="store_true", dest="foreground", default=False, help="Run this command in the foreground.")
    advanced.add_option("--noprefetch", action="store_false", dest="prefetch", default=True, help="Disable auto-transcoding of files that we expect to be read soon.")
    advanced.add_option("--threads", action="store", dest="threads", default=cpu_count(), type="int", help="How many threads should we use? This should probably be set to the number of cores you have available. Default: %default")
    advanced.add_option("--cache-time", action="store", dest="cachetime", default=60, type="int", help="How may seconds should we keep the transcoded MP3s in RAM after they are last touched? 0 removes them as soon as the file descriptor is released.")
    advanced.add_option("--normalize-perms", action="store_true", dest="modperms", default=False, help="Should we present all files and folders as read only?")
    devel.add_option("--always-conv", action="store_true", dest="attrbconv", default=False, help="Will convert flac->mp3 even for things like 'ls'. Only needed if you want 'ls' to show the right file size, but doing so will be very slow. (You are forcing vbrfs to transcode a directory just to 'ls'.)")
    devel.add_option("--debug", action="store_true", dest="debug", default=False, help="Log every action sent to the filesystem.")
    devel.add_option("--log-file", action="store", dest="logfile", default="/tmp/vbrfs.log", help="The file to log to.")

    # Options, parse 'em
    (options, args) = parser.parse_args()

    # Set up the logger
    handler = None
    if options.logfile == "-":
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(options.logfile, "w", encoding = "UTF-8")
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    if options.debug:
        logger.setLevel(logging.DEBUG)

    # Set up the minimal options
    if options.minimal:
        options.prefetch = False
        options.threads = 1
        options.cachetime = 30
        options.keep_on_release = False

    # Check for invalid command line options
    if len(args) < 2:
        print "You must specify the flac directory and the mp3 directory."
        sys.exit(1)
    if len(args) > 2:
        print "Did you accidentally leave out an option? I don't accept three arguments."
        sys.exit(2)
    if not os.path.isdir(args[0]):
        print "The FLAC folder you specified (%s) doesn't exist." % args[0]
        sys.exit(3)
    if not os.path.isdir(args[1]):
        print "The target mount point (%s) doesn't exist." % args[1]
        sys.exit(4)

    # Test that FLAC and LAME commands are installed.
    try:
        test_cmd = subprocess.Popen(['metaflac', '--version'],stdout=subprocess.PIPE)
        test_cmd.wait()
        logger.info("Detected metaflac executable. Version: %s" % test_cmd.stdout.read().rstrip())
    except OSError:
        print "You don't seem to have metaflac installed. You must install the flac package.\nDebian derived distro: sudo apt-get install flac\nRedhat derived distro: sudo yum install flac"
        sys.exit(5)

    # Test that FLAC and LAME commands are installed.
    try:
        test_cmd = subprocess.Popen(['lame', '--version'],stdout=subprocess.PIPE)
        test_cmd.wait()
        logger.info("Detected lame executable. Version: %s" % test_cmd.stdout.readline().rstrip())
    except OSError:
        print "You don't seem to have lame installed. You must install the lame package.\nDebian derived distro: sudo apt-get install lame\nRedhat derived distro: sudo yum install lame"
        sys.exit(6)

    def start():
        # Make the magic happen
        vbr = vbrConvert(args[0])
        # Trying to run in the background with the fuse module is broken and I don't know why so we fork-exec instead
        FUSE(vbr, args[1], foreground=True)

        # Tell any encoding processes to quit
        with vbr.slock:
            for key in vbr.known_files:
                with vbr.known_files[key].lock:
                    vbr.known_files[key].status = -1

        sys.exit(0)

    # Run in the foreground
    if options.foreground:
        start()

    # Run in the background
    if os.fork() == 0:
        start()
    sys.exit(0)
