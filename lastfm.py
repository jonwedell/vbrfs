#!/usr/bin/python

# vbrfs - A real-time FLAC to mp3-vbr fuse filesystem written in python.
# scrobble.py - To upload your listens to last.fm
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
from hashlib import md5
from time import time as unixtime
import requests
import webbrowser

# Store the api_key and secret using ROT26 encryption to prevent theft
api_key = 'f9629d0d0a7d28285d07878c400db123'
secret = 'a58577f64c88d9dab10ec8214e1bb36a'
session_key = None

# Configuration parameters
request_timeout = 5
request_url = "http://ws.audioscrobbler.com/2.0/"

def add_secret(payload):
    url = u""
    for key in sorted(payload):
        url += u"%s%s" % (key, payload[key])
    url += secret

    payload['api_sig'] = unicode(md5(url).hexdigest(), "utf-8")
    return

def scrobble(tags, init_time):

    payload = {'api_key': api_key, 'method': 'track.scrobble', 'sk': session_key}

    # We can't scrobble if we don't know the artist and the track
    try:
        payload['artist'] = tags['artist']
        payload['track'] = tags['title']
    except KeyError:
        return (1,"Missing artist or title. Artist: %s Title: %s" % (tags.get('artist',"?"),tags.get('title',"?")))

    # Add optional tags
    if tags.get('album', None):
        payload['album'] = tags['album']
    if tags.get('tracknumber', None):
        payload['tracknumber'] = tags['tracknumber']
    if tags.get('duration', None):
        payload['duration'] = int(tags['duration'])

    # Subtract duration to get start time?
    payload['timestamp'] = int(init_time)

    # Finish formatting the request
    add_secret(payload)
    payload['format'] = 'json'

    # Scrobble
    try:
        r = requests.post(request_url, params=payload, timeout=request_timeout)
    except requests.Timeout:
        return (-1, "Timeout occured while attempting to connect to last.fm")

    # Process the request, look for errors
    if r.status_code != 200:
        return (r.status_code,r.text)

    json = r.json()

    # Check for an error message
    try:
        if json.get('error',None):
            return (int(json['error']), json['message'])
    except KeyError:
        pass
    finally:
        return (200,json)

def initialize():

    print "Authorizing VBRFS with last.fm..."

    # First we need an auth token
    payload = {'api_key': api_key, 'format': 'json', 'method': 'auth.gettoken'}
    r = requests.get(request_url, params=payload, timeout=10)

    token = r.json()['token']

    # Then we send them to the website to approve
    webbrowser.open("http://www.last.fm/api/auth/?api_key=%s&token=%s" % (api_key,token), new=1)
    print "Please authorize VBRFS to scrobble using the page that should have opened in your web browser.\nIf you do not see a page, please go to: http://www.last.fm/api/auth/?api_key=%s&token=%s\n" % (api_key,token)
    raw_input("Press enter once you have authorized VBRFS: ")

    # Then we can get the session key
    payload = {'api_key': api_key, 'method': 'auth.getSession', 'token': token}
    add_secret(payload)
    payload['format'] = 'json'
    r = requests.get(request_url, params=payload, timeout=10)

    # Save the session key for future use
    session_key = r.json()['session']['key']
    open(os.path.expanduser("~/.vbrfs_lastfm_key"), "w").write(session_key)

    print "Done!"

    return session_key

if not os.path.isfile(os.path.expanduser("~/.vbrfs_lastfm_key")) or __name__ == '__main__':
    session_key = initialize()
else:
    session_key = open(os.path.expanduser("~/.vbrfs_lastfm_key"), "r").read()
