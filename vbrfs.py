#!/usr/bin/python

# vbrfs.py - A real-time FLAC to mp3-vbr fuse filesystem written in python.
# Copyright (c) 2013,2014 Jonathan Wedell <jonwedell@gmail.com> (author)
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License along
#    with this program; if not, write to the Free Software Foundation, Inc.,
#    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import os
import sys
import time
import stat
import math
import errno
import logging
import logging.handlers
import StringIO
import optparse
import threading
import subprocess
from errno import *

from multiprocessing import cpu_count
from fuse import FUSE, FuseOSError, Operations

def _extensionConvert(the_path, mode="encode"):

    if mode == "encode":
        if options.enc_format == "mp3":
            return the_path.replace('.flac','.mp3').replace('.FLAC','.MP3')
        elif options.enc_format == "ogg":
            return the_path.replace('.flac','.ogg').replace('.FLAC','.OGG')
    elif mode == "decode":
        if options.enc_format == "mp3":
            return the_path.replace('.mp3','.flac').replace('.MP3','.FLAC')
        elif options.enc_format == "ogg":
            return the_path.replace('.ogg','.flac').replace('.OGG','.FLAC')
    else:
        raise ValueError("Invalid mode.")

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

                            if conv_obj.opens > 0:
                                logger.debug("Cache expired on (%s) but keeping because file is still open.", conv_obj.path)
                                conv_obj.last_read = now
                            else:
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
        self.opens = 0
        self.init_time = time.time()

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

        # Need to check these cases eventually, but as the commented out portion stands it causes the whole getup to error out
        # First get the tags
        #if not os.access(self.path, os.F_OK):
            #raise OSError(2,"No such file or directory.")
        #if not os.access(self.path,os.R_OK):
            #raise OSError(2,"No read permissions on the file %s." % self.path)

        tag_cmd = subprocess.Popen(['metaflac', '--show-total-samples', '--show-sample-rate', '--export-tags-to', '-', self.path],stdout=subprocess.PIPE)
        tags = {}

        # Get the track duration from the sample information
        tags['duration'] = float(tag_cmd.stdout.readline()) / float(tag_cmd.stdout.readline())

        for sp in map(lambda x:x.partition("="), tag_cmd.stdout.read().split("\n")):
            if sp[0]:
                tags[sp[0].lower()] = sp[2]
        tag_cmd.wait()

        with self.lock:
            self.tags = tags

        # Then get the decoded FLAC stream
        flac = subprocess.Popen(['flac', '-c', '-d', self.path],stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if options.enc_format == "mp3":
            # Then set up the lame arguments and call lame
            lamecmd = ['lame','-q0', '-V', options.v, '--vbr-new', '--ignore-tag-errors', '--add-id3v2', '--pad-id3v2', '--ignore-tag-errors', '--ta', tags.get('artist','?'), '--tt', tags.get('title','?'), '--tl', tags.get('album','?'), '--tg', tags.get('genre','?'), '--tn', tags.get('tracknumber','?'), '--ty', tags.get('date','?'), '-', '-']
            lame = subprocess.Popen(lamecmd,stdout=subprocess.PIPE,stdin=flac.stdout,stderr=subprocess.PIPE)
        elif options.enc_format == "ogg":
            # Then set up the lame arguments and call ogg
            lamecmd = ['oggenc', '-q', options.q, '--artist', tags.get('artist','?'), '--title', tags.get('title','?'), '--album', tags.get('album','?'), '--genre', tags.get('genre','?'), '--tracknum', tags.get('tracknumber','?'), '--date', tags.get('date','?'), '-']
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
            return _extensionConvert(os.path.abspath(self._full_path(partial)), mode="decode")

    def convFile(self, path, getnext=True):
        # We don't need to convert a given file twice

        if getnext: logger.debug("convFile( " + unicode(path) + " , " + unicode(getnext) + " )")

        with self.slock:
            if path in self.known_files:
                return self.known_files[path]

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
                the_path = _extensionConvert(os.path.join(os.path.dirname(path),next_files[x]), mode="encode")
                logger.debug("prefetch( " + unicode(the_path) + " )")
                self.convFile(the_path,getnext=False)

        return conv_obj


    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        logger.debug("getattr( " + unicode(path) + " , " + unicode(fh) + " )")

        # Make sure they can't grab the actual FLACs
        if path[-5:] == ".flac" or path[-5:] == ".FLAC":
            raise OSError(2,"No such file or directory.")

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
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid', 'st_blocks', 'st_blksize'))

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

        # Estimate the size if we don't actually know
        if size is False and not os.path.exists(_extensionConvert(self._absolutePath(path), mode="encode")):
            res['st_size'] = int((res['st_size'] / 2.5))

        # Update the number of blocks and the ideal block size
        res['st_blocks'] = int(math.ceil(res['st_size'] / 512))
        res['st_blksize'] = 32

        # Return the results
        return res

    def readdir(self, path, fh):
        logger.debug("readdir( " + unicode(path) + " , " + unicode(fh) + " )")
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield _extensionConvert(r, mode="encode")

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
        conv_obj = self.convFile(path)

        # Increment the "locks" on the file
        if conv_obj is not None:
            with conv_obj.lock:
                conv_obj.opens += 1

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

        # If it is a real file, just close it and move on
        if os.path.isfile(self._full_path(path)) or os.path.isdir(self._full_path(path)):
            os.close(fh)

            # TODO: This unfortunately can't be here. I think I need to make non-transcode files objects as well....
            future_code = """
            # We would scrobble the song here, if we knew how long it was open
            tag_cmd = subprocess.Popen(['id3v2', '-R', self._full_path(path)],stdout=subprocess.PIPE)
            tags = {}
            tag_map = {"TRCK":"tracknumber", "TIT2": "title", "TPE2": "artist", "TALB": "album"}

            for sp in map(lambda x:x.partition(":"), tag_cmd.stdout.read().split("\n")):
                if sp[0]:
                    if tag_map.get(sp[0].upper(),None):
                        tags[tag_map[sp[0].upper()]] = sp[2]
            tag_cmd.wait()
            """

            return

        # It is a transcode file. Decrease the locks on it first.
        with self.slock:
            conv_obj = self.known_files.get(path,None)
        if conv_obj is not None:
            with conv_obj.lock:
                conv_obj.opens -= 1

                # They have close the file for the "last" time
                if conv_obj.opens == 0:

                    # Remove the transcode on file close if they specified the read once argument
                    if not options.keep_on_release:
                        conv_obj.status = -1

                    # Scrobble to last.fm
                    if options.lastfm and hasattr(conv_obj,"tags"):
                        play_time = time.time() - conv_obj.init_time
                        if play_time > conv_obj.tags['duration']/2 and play_time > 30:

                            response = lastfm.scrobble(conv_obj.tags,conv_obj.init_time)
                            if response[0] != 200:
                                logger.warn("Scrobble error with code %d: %s" % (response[0],response[1]))
                            else:
                                logger.debug("Successfully scrobbled '%s' by '%s'." % (conv_obj.tags.get("title","?"),conv_obj.tags.get("artist","?")))

        # Only close the file if it is a real file
        if os.path.isfile(self._full_path(path)) or os.path.isdir(self._full_path(path)):
            os.close(fh)

if __name__ == '__main__':

    # Specify some basic information about our command
    usage = "usage: %prog [options] flacdir vbrdir"
    parser = optparse.OptionParser(usage=usage,version="%prog 1.0",description="This program will present all FLACS as VBR mp3s or oggs. Like mp3fs but with VBR. It will add basic idv2 tags but it will not transfer all tags.")

    # Set up the option groups
    basic = optparse.OptionGroup(parser,"Basic options","If the default isn't good enough for you.")
    parser.add_option_group(basic)
    encoder_options = optparse.OptionGroup(parser,"Encoder options","Allows you to encode to Ogg rather than mp3, as well as specify encoding level.")
    parser.add_option_group(encoder_options)
    advanced = optparse.OptionGroup(parser,"Advanced options","You may want to use some of these.")
    parser.add_option_group(advanced)
    devel = optparse.OptionGroup(parser,"Developer options","You really shouldn't use these unless you know what you are doing.")
    parser.add_option_group(devel)

    # Specify the common arguments
    basic.add_option("--minimal", action="store_true", dest="minimal", default=False, help="Automatically chooses options to allow this to work on low power machines. Implies --noprefetch, --threads 1, and --cachetime 30.")
    basic.add_option("--lastfm", action="store_true", dest="lastfm", default=False, help="Scrobble plays to last.fm. Will require a brief authorization step the first time it is used.")
    encoder_options.add_option("--format", action="store", dest="enc_format", type="choice", default="mp3", choices=("mp3","ogg"), help="What lossy format should we transcode into? Default: %default.")
    encoder_options.add_option("-v","--V", action="store", dest="v", type="choice", default="2", choices=(map(lambda x:str(x),range(0,10))), help="What v level mp3 do you want to transcode to? Default: %default.")
    encoder_options.add_option("-q","--Q", action="store", dest="q", type="choice", default="6", choices=(map(lambda x:str(x),range(0,10))), help="What q level Ogg vorbis do you want to transcode to? Default: %default.")
    advanced.add_option("--no-multiread", action="store_false", dest="keep_on_release", default=True, help="Free a transcode from RAM after it has been read. (Otherwise it will be held until the cache timeout.) Useful if you will only read each file once.")
    advanced.add_option("--foreground", action="store_true", dest="foreground", default=False, help="Run this command in the foreground.")
    advanced.add_option("--noprefetch", action="store_false", dest="prefetch", default=True, help="Disable auto-transcoding of files that we expect to be read soon.")
    advanced.add_option("--threads", action="store", dest="threads", default=cpu_count(), type="int", help="How many threads should we use? This should probably be set to the number of cores you have available. Default: %default")
    advanced.add_option("--cache-time", action="store", dest="cachetime", default=60, type="int", help="How may seconds should we keep the transcoded files in RAM after they are last touched? 0 removes them as soon as the file descriptor is released.")
    advanced.add_option("--normalize-perms", action="store_true", dest="modperms", default=False, help="Should we present all files and folders as read only?")
    devel.add_option("--always-conv", action="store_true", dest="attrbconv", default=False, help="Will convert flac->vbr format even for things like 'ls'. Only needed if you want 'ls' to show the right file size, but doing so will be very slow. (You are forcing vbrfs to transcode a directory just to 'ls'.)")
    devel.add_option("--quiet", action="store_false", dest="debug", default=True, help="Only log critical events.")
    devel.add_option("--log-file", action="store", dest="logfile", help="The file to log to.")
    devel.add_option("--log-size", action="store", dest="logsize", default=4194304, type="int", help="How many bytes can the log grow to before being rotated?")

    # Options, parse 'em
    (options, args) = parser.parse_args()

    # Set up the logger
    handler = None
    # Log to STDOUT by default in foreground mode or /tmp/vbrfs.log in background
    if options.logfile is None:
        if options.foreground:
            options.logfile = "-"
        else:
            options.logfile = "/tmp/vbrfs.log"

    if options.logfile == "-":
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.handlers.RotatingFileHandler(options.logfile, "w", encoding = "UTF-8", maxBytes=options.logsize)
    formatter = logging.Formatter("%(asctime)s:%(module)s_%(lineno)d:%(levelname)s: %(message)s")

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

    # Check that the specified enough/the right arguments and that the mount point is up to spec
    if len(args) < 2:
        print "Error: You must specify the FLAC source directory and the target directory."
        logger.critical("Could not run: You must specify FLAC the source directory and the target directory.")
        sys.exit(1)
    if len(args) > 2:
        print "Error: Did you accidentally leave out an option? I don't accept three arguments."
        logger.critical("Could not run: Did you accidentally leave out an option? I don't accept three arguments.")
        sys.exit(2)
    if not os.path.isdir(args[0]):
        print "Error: The FLAC folder you specified (%s) doesn't exist." % os.path.abspath(args[0])
        logger.critical("Could not run: The FLAC folder you specified (%s) doesn't exist." % os.path.abspath(args[0]))
        sys.exit(3)
    if not os.path.exists(args[1]):
        print "Error: The target mount point (%s) doesn't exist." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) doesn't exist." % os.path.abspath(args[1]))
        sys.exit(4)
    if not os.path.isdir(args[1]):
        print "Error: The target mount point (%s) exists but isn't a directory." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) exists but isn't a directory." % os.path.abspath(args[1]))
        sys.exit(5)
    if os.path.ismount(args[1]):
        print "Error: The target mount point (%s) appears to already have something mounted there." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) appears to already have something mounted there." % os.path.abspath(args[1]))
        sys.exit(6)
    if os.listdir(args[1]):
        print "Error: The target mount point (%s) is not empty." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) is not empty." % os.path.abspath(args[1]))
        sys.exit(7)
    if not os.path.isdir(args[1]):
        print "Error: The target mount point (%s) doesn't exist." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) doesn't exist." % os.path.abspath(args[1]))
        sys.exit(4)
    if os.path.ismount(args[1]):
        print "Error: The target mount point (%s) appears to already have something mounted there." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) appears to already have something mounted there." % os.path.abspath(args[1]))
        sys.exit(5)
    if os.listdir(args[1]):
        print "Error: The target mount point (%s) is not empty." % os.path.abspath(args[1])
        logger.critical("Could not run: The target mount point (%s) is not empty." % os.path.abspath(args[1]))
        sys.exit(6)

    # Test that FLAC and LAME commands are installed.
    def checkCommand(cmd_name, pkg_name=None):
        if pkg_name is None:
            pkg_name = cmd_name
        try:
            test_cmd = subprocess.Popen([cmd_name, '--version'],stdout=subprocess.PIPE)
            test_cmd.wait()
            logger.debug("Detected %s executable. Version: %s" % (cmd_name,test_cmd.stdout.readline().rstrip()))
        except OSError:
            print "You don't seem to have %s installed. You must install the %s package.\nDebian derived distro: sudo apt-get install %s\nRedhat derived distro: sudo yum install %s" % (cmd_name,pkg_name,pkg_name,pkg_name)
            logger.critical("Could not run: Missing package: %s" % pkg_name)
            sys.exit(7)

    checkCommand("fusermount","fuse")
    checkCommand("flac")
    checkCommand("metaflac","flac")
    checkCommand("lame")

    if options.enc_format == "ogg":
        checkCommand("oggenc","vorbis-tools")

    def start():
        # Make the magic happen
        vbr = vbrConvert(args[0])

        # Trying to run in the background with the fuse module is broken and I don't know why so we fork-exec instead
        try:
            FUSE(vbr, args[1], foreground=True)
        except RuntimeError as e:
            logger.critical("We encountered a runtime error when attempting to run FUSE.")
            if not options.foreground:
                print "Something is wrong with the location you are attempting to mount onto."
            sys.exit(8)

        # Tell any encoding processes to quit
        with vbr.slock:
            for key in vbr.known_files:
                with vbr.known_files[key].lock:
                    vbr.known_files[key].status = -1

        sys.exit(0)

    # If they want to use last.fm, we need to initialize it prior to starting up the FS.
    if options.lastfm or os.path.isfile(os.path.expanduser("~/.vbrfs_lastfm_key")):
        try:
            import lastfm
        except ImportError:
            print "You don't seem to have the python requests module installed. It is required to use last.fm scrobbling.\nPython direct install: sudo easy_install requests\nDebian derived distro: sudo apt-get install python-requests\n"
            logger.critical("Could not run. Missing requests module and last.fm support enabled.")
            sys.exit(9)

    # Run in the foreground
    if options.foreground:
        start()

    # Run in the background
    if os.fork() == 0:
        start()
    sys.exit(0)
