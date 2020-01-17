#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import urllib
import urllib.request
import shutil
import tempfile
import filecmp
import hashlib
import calendar, time


HASHBLOCKSIZE = 65536

# Inspired in https://stackoverflow.com/a/59602931
# and https://stackoverflow.com/a/15035466
def download_file(url, local_filename, local_stats={}):
	tmp_file = None
	headers = {}
	if ('ETag' in local_stats) or ('updated' in local_stats) or os.path.isfile(local_filename):
		if 'ETag' in local_stats:
			headers["If-None-Match"] = local_stats['ETag']
		else:
			if 'updated' in local_stats:
				timestr = local_stats['updated']
			else:
				timestamp = os.path.getmtime(local_filename)
				timestr = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(timestamp))
			headers["If-Modified-Since"] = timestr
		tmp_file = tempfile.NamedTemporaryFile()
		down_filename = tmp_file.name
	else:
		down_filename = local_filename
	
	req = urllib.request.Request(url,headers=headers)
	
	down_sha1 = None
	down_last_modified = None
	down_ETag = None
	try:
		got_headers = None
		with urllib.request.urlopen(req) as url_fh, open(down_filename,mode='wb') as file_fh:
			local_stats['url'] = url_fh.geturl()
			shutil.copyfileobj(url_fh, file_fh)
			got_headers = url_fh.info()
		
		down_ETag = got_headers.get('ETag')
		
		# Computing this is needed in any case
		h = hashlib.sha1()
		with open(down_filename,mode='rb') as down_fh:
			blk = down_fh.read(HASHBLOCKSIZE)
			while len(blk) > 0:
				h.update(blk)
				blk = down_fh.read(HASHBLOCKSIZE)
		
		down_sha1 = h.hexdigest()
		
		# Should we check whether it is different?
		if tmp_file:
			if 'sha1' in local_stats:
				isNewer = local_stats['sha1'] != down_sha1
			else:
				isNewer = (os.stat(local_filename).st_size != os.stat(down_filename).st_size) or not filecmp.cmp(local_filename,down_filename, shallow=False)
			
			if isNewer:
				down_mtime = os.path.getmtime(down_filename)
				shutil.move(down_filename,local_filename)
				# This is needed to avoid an spurious exception
				with open(down_filename,mode='wb') as _:
					pass
			else:
				down_mtime = None
				local_filename = None
		
		if (local_filename is not None) and ('Last-Modified' in got_headers):
			mtime = calendar.timegm(time.strptime(got_headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S GMT'))
			os.utime(local_filename, (mtime, mtime))
		
		down_last_modified = got_headers['Last-Modified']  if 'Last-Modified' in got_headers  else  time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(down_mtime))
	except urllib.error.HTTPError as e:
		if e.code == 304:
			local_filename = None
		else:
			raise e
	finally:
		# Anyway, updating the output stats
		if down_sha1 is not None:
			local_stats['sha1']  = down_sha1
		if down_last_modified is not None:
			local_stats['updated'] = down_last_modified
		if down_ETag is not None:
			local_stats['ETag'] = down_ETag
		
		if tmp_file is not None:
			tmp_file.close()
	
	return local_filename, local_stats

if __name__ == "__main__":
	if len(sys.argv) >= 3:
		url = sys.argv[1]
		dest = sys.argv[2]
		
		stats = {}
		if len(sys.argv) >= 4:
			stats['ETag'] = sys.argv[3]
			if len(sys.argv) >= 5:
				stats['sha1'] = sys.argv[4]
				if len(sys.argv) >= 6:
					stats['updated'] = sys.argv[5]
		
		got_filename, got_stats = download_file(url,dest,stats)
		if got_filename:
			print("Got fresh copy of {} into {} ({}) ({}) ({})".format(url,dest,got_stats.get('ETag'),got_stats.get('sha1'),got_stats.get('updated')))
		else:
			print("Content at {} has not changed, no update of {} is needed ({}) ({}) ({})".format(url,dest,got_stats.get('ETag'),got_stats.get('sha1'),got_stats.get('updated')))
	else:
		print("Usage: {0} {{url}} {{destination file}} [{ETag} [{hex SHA1 hash} [update date]]]".format(sys.argv[0]))