/* 
   dbench version 1.2
   Copyright (C) 1999 by Andrew Tridgell <tridge@samba.org>
   Copyright (C) 2001 by Martin Pool <mbp@samba.org>
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#include "dbench.h"

#define MAX_FILES 1000

static char buf[70000];
extern int line_count;

char *server = NULL;
extern int sync_open, sync_dirs;

static struct {
	int fd;
	int handle;
} ftable[MAX_FILES];


/* Find the directory holding a file, and flush it to disk.  We do
   this in -S mode after a directory-modifying mode, to simulate the
   way knfsd tries to flush directories.  MKDIR and similar operations
   are meant to be synchronous on NFSv2. */
void sync_parent(char *fname)
{
	char *copy_name;
	int dir_fd;
	char *slash;

	if (strchr(fname, '/')) {
		copy_name = strdup(fname);
		slash = strrchr(copy_name, '/');
		*slash = '\0';
	} else {
		copy_name = strdup(".");
	} 
	
	dir_fd = open(copy_name, O_RDONLY);
	if (dir_fd == -1) {
		printf("open directory \"%s\" for sync failed: %s\n",
		       copy_name,
		       strerror(errno));
	} else {
		if (fdatasync(dir_fd) == -1) {
			printf("datasync directory \"%s\" failed: %s\n",
			       copy_name,
			       strerror(errno));
		}
		if (close(dir_fd) == -1) {
			printf("close directory failed: %s\n",
			       strerror(errno));
		}
	}
	free(copy_name);
}


void nb_setup(int client)
{
	/* nothing to do */
}

void nb_unlink(char *fname)
{
	strupper(fname);

	if (unlink(fname) != 0) {
		printf("(%d) unlink %s failed (%s)\n", 
		       line_count, fname, strerror(errno));
	}
	if (sync_dirs)
		sync_parent(fname);
}

void expand_file(int fd, int size)
{
	int s;
	while (size) {
		s = MIN(sizeof(buf), size);
		write(fd, buf, s);
		size -= s;
	}
}

void nb_open(char *fname, int handle, int size)
{
	int fd, i;
	int flags = O_RDWR|O_CREAT;
	struct stat st;
	static int count;

	strupper(fname);

	if (size == 0) flags |= O_TRUNC;

	if (sync_open)
		flags |= O_SYNC;
	
	fd = open(fname, flags, 0600);
	if (fd == -1) {
		printf("(%d) open %s failed for handle %d (%s)\n", 
		       line_count, fname, handle, strerror(errno));
		return;
	}
	fstat(fd, &st);
	if (size > st.st_size) {
#if DEBUG
		printf("(%d) expanding %s to %d from %d\n", 
		       line_count, fname, size, (int)st.st_size);
#endif
		expand_file(fd, size - st.st_size);
	} else if (size < st.st_size) {
		printf("truncating %s to %d from %d\n", 
		       fname, size, (int)st.st_size);
		ftruncate(fd, size);
	}
	for (i=0;i<MAX_FILES;i++) {
		if (ftable[i].handle == 0) break;
	}
	if (i == MAX_FILES) {
		printf("file table full for %s\n", fname);
		exit(1);
	}
	ftable[i].handle = handle;
	ftable[i].fd = fd;
	if (count++ % 100 == 0) {
		printf(".");
	}
}

void nb_write(int handle, int size, int offset)
{
	int i;

	if (buf[0] == 0) memset(buf, 1, sizeof(buf));

	for (i=0;i<MAX_FILES;i++) {
		if (ftable[i].handle == handle) break;
	}
	if (i == MAX_FILES) {
#if 1
		printf("(%d) nb_write: handle %d was not open size=%d ofs=%d\n", 
		       line_count, handle, size, offset);
#endif
		return;
	}
	lseek(ftable[i].fd, offset, SEEK_SET);
	if (write(ftable[i].fd, buf, size) != size) {
		printf("write failed on handle %d\n", handle);
	}
}

void nb_read(int handle, int size, int offset)
{
	int i;
	for (i=0;i<MAX_FILES;i++) {
		if (ftable[i].handle == handle) break;
	}
	if (i == MAX_FILES) {
		printf("(%d) nb_read: handle %d was not open size=%d ofs=%d\n", 
		       line_count, handle, size, offset);
		return;
	}
	lseek(ftable[i].fd, offset, SEEK_SET);
	read(ftable[i].fd, buf, size);
}

void nb_close(int handle)
{
	int i;
	for (i=0;i<MAX_FILES;i++) {
		if (ftable[i].handle == handle) break;
	}
	if (i == MAX_FILES) {
		printf("(%d) nb_close: handle %d was not open\n", 
		       line_count, handle);
		return;
	}
	close(ftable[i].fd);
	ftable[i].handle = 0;
}

void nb_mkdir(char *fname)
{
	strupper(fname);

	if (mkdir(fname, 0700) != 0) {
#if DEBUG
		printf("mkdir %s failed (%s)\n", 
		       fname, strerror(errno));
#endif
	}
	if (sync_dirs)
		sync_parent(fname);
}

void nb_rmdir(char *fname)
{
	strupper(fname);

	if (rmdir(fname) != 0) {
		printf("rmdir %s failed (%s)\n", 
		       fname, strerror(errno));
	}
	if (sync_dirs)
		sync_parent(fname);
}

void nb_rename(char *old, char *new)
{
	strupper(old);
	strupper(new);

	if (rename(old, new) != 0) {
		printf("rename %s %s failed (%s)\n", 
		       old, new, strerror(errno));
	}
	if (sync_dirs)
		sync_parent(new);
}


void nb_stat(char *fname, int size)
{
	struct stat st;

	strupper(fname);

	if (stat(fname, &st) != 0) {
		printf("(%d) nb_stat: %s size=%d %s\n", 
		       line_count, fname, size, strerror(errno));
		return;
	}
	if (S_ISDIR(st.st_mode)) return;

	if (st.st_size != size) {
		printf("(%d) nb_stat: %s wrong size %d %d\n", 
		       line_count, fname, (int)st.st_size, size);
	}
}

void nb_create(char *fname, int size)
{
	nb_open(fname, 5000, size);
	nb_close(5000);
}
