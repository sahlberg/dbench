/* 
   dbench version 2
   Copyright (C) Andrew Tridgell 1999
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <time.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#ifdef HAVE_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif

#include <sys/param.h>
#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif
#include <utime.h>
#include <errno.h>
#include <strings.h>
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>

#if HAVE_ATTR_XATTR_H
#include <attr/xattr.h>
#elif HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#elif HAVE_SYS_ATTRIBUTES_H
#include <sys/attributes.h>
#endif

#ifdef HAVE_SYS_EXTATTR_H
#include <sys/extattr.h>
#endif

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#ifndef MSG_WAITALL
#define MSG_WAITALL 0x100
#endif

#define PRINT_FREQ 1

#ifndef MIN
#define MIN(x,y) ((x)<(y)?(x):(y))
#endif

#define TCP_PORT 7003
#define TCP_OPTIONS "TCP_NODELAY SO_REUSEADDR"

#define BOOL int
#define True 1
#define False 0
#define uint32 unsigned

struct op {
	unsigned count;
	double total_time;
	double max_latency;
};

struct child_struct {
	int id;
	int failed;
	int line;
	int done;
	int cleanup;
	int cleanup_finished;
	const char *directory;
	double bytes;
	double bytes_done_warmup;
	double max_latency;
	double worst_latency;
	struct timeval starttime;
	struct timeval lasttime;
	off_t bytes_since_fsync;
	char *cname;
	struct {
		double last_bytes;
		struct timeval last_time;
	} rate;
	struct opnames {
		struct op op_NTCreateX;
		struct op op_Close;
		struct op op_Rename;
		struct op op_Unlink;
		struct op op_Deltree;
		struct op op_Rmdir;
		struct op op_Mkdir;
		struct op op_Qpathinfo;
		struct op op_Qfileinfo;
		struct op op_Qfsinfo;
		struct op op_Sfileinfo;
		struct op op_Find;
		struct op op_WriteX;
		struct op op_ReadX;
		struct op op_LockX;
		struct op op_UnlockX;
		struct op op_Flush;
		struct op op_GETATTR3;
		struct op op_MKDIR3;
		struct op op_RMDIR3;
		struct op op_LOOKUP3;
		struct op op_CREATE3;
		struct op op_WRITE3;
		struct op op_COMMIT3;
		struct op op_READ3;
		struct op op_ACCESS3;
		struct op op_FSSTAT3;
		struct op op_FSINFO3;
		struct op op_SYMLINK3;
		struct op op_REMOVE3;
		struct op op_READDIRPLUS3;
		struct op op_LINK3;
	} op;
	void *private;
};

struct options {
	int nprocs;
	int sync_open;
	int sync_dirs;
	int do_fsync;
	int no_resolve;
	int fsync_frequency;
	char *tcp_options;
	int timelimit;
	int warmup;
	const char *directory;
	char *loadfile;
	double targetrate;
	int ea_enable;
	const char *server;
	int clients_per_process;
	int one_byte_write_fix;
	int stat_check;
	int fake_io;
	int skip_cleanup;
	int per_client_results;
	const char *export;
	const char *protocol;
};

struct nb_operations {
	void (*setup)(struct child_struct *);
	void (*deltree)(struct child_struct *, const char *dname);
	void (*cleanup)(struct child_struct *child);

	/* CIFS operations */
	void (*flush)(struct child_struct *, int handle, const char *status);
	void (*close)(struct child_struct *, int handle, const char *status);
	void (*lockx)(struct child_struct *, int handle, uint32_t offset, int size, const char *status);
	void (*rmdir)(struct child_struct *, const char *fname, const char *status);
	void (*mkdir)(struct child_struct *, const char *dname, const char *status);
	void (*rename)(struct child_struct *, const char *old, const char *new, const char *status);
	void (*readx)(struct child_struct *, int handle, int offset, int size, int ret_size, const char *status);
	void (*writex)(struct child_struct *, int handle, int offset, int size, int ret_size, const char *status);
	void (*unlink)(struct child_struct *, const char *fname, int attr, const char *status);
	void (*unlockx)(struct child_struct *child,int handle, uint32_t offset, int size, const char *status);
	void (*findfirst)(struct child_struct *child, const char *fname, int level, int maxcnt, int count, const char *status);
	void (*sfileinfo)(struct child_struct *child, int handle, int level, const char *status);
	void (*qfileinfo)(struct child_struct *child, int handle, int level, const char *status);
	void (*qpathinfo)(struct child_struct *child, const char *fname, int level, const char *status);
	void (*qfsinfo)(struct child_struct *child, int level, const char *status);
	void (*createx)(struct child_struct *child, const char *fname, uint32_t create_options, uint32_t create_disposition, int fnum, const char *status);

	/* NFSv3 operations */
	void (*getattr3)(struct child_struct *child, const char *fname, const char *status);
	void (*lookup3)(struct child_struct *child, const char *fname, const char *status);
	void (*create3)(struct child_struct *child, const char *fname, const char *status);
	void (*write3)(struct child_struct *child, const char *fname, int offset, int len, int stable, const char *status);
	void (*commit3)(struct child_struct *child, const char *fname, const char *status);
	void (*read3)(struct child_struct *child, const char *fname, int offset, int len, const char *status);
	void (*access3)(struct child_struct *child, const char *fname, int desired, int granted, const char *status);
	void (*mkdir3)(struct child_struct *child, const char *fname, const char *status);
	void (*rmdir3)(struct child_struct *child, const char *fname, const char *status);
	void (*fsstat3)(struct child_struct *child, const char *status);
	void (*fsinfo3)(struct child_struct *child, const char *status);
	void (*symlink3)(struct child_struct *child, const char *fname, const char *fname2, const char *status);
	void (*remove3)(struct child_struct *child, const char *fname, const char *status);
	void (*readdirplus3)(struct child_struct *child, const char *fname, const char *status);
	void (*link3)(struct child_struct *child, const char *fname, const char *fname2, const char *status);
	void (*rename3)(struct child_struct *child, const char *fname, const char *fname2, const char *status);
};
extern struct nb_operations nb_ops;

/* CreateDisposition field. */
#define FILE_SUPERSEDE 0
#define FILE_OPEN 1
#define FILE_CREATE 2
#define FILE_OPEN_IF 3
#define FILE_OVERWRITE 4
#define FILE_OVERWRITE_IF 5

/* CreateOptions field. */
#define FILE_DIRECTORY_FILE       0x0001
#define FILE_WRITE_THROUGH        0x0002
#define FILE_SEQUENTIAL_ONLY      0x0004
#define FILE_NON_DIRECTORY_FILE   0x0040
#define FILE_NO_EA_KNOWLEDGE      0x0200
#define FILE_EIGHT_DOT_THREE_ONLY 0x0400
#define FILE_RANDOM_ACCESS        0x0800
#define FILE_DELETE_ON_CLOSE      0x1000

#ifndef O_DIRECTORY
#define O_DIRECTORY    0200000
#endif

#include "proto.h"

extern struct options options;
