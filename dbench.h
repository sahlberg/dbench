/* -*-  mode:c; tab-width:8; c-basic-offset:8; indent-tabs-mode:nil;  -*- */
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
#define _FILE_OFFSET_BITS 64

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

#define True 1
#define False 0

struct op {
	unsigned count;
	double total_time;
	double max_latency;
};

#define ZERO_STRUCT(x) memset(&(x), 0, sizeof(x))

#define MAX_OPS 100

struct child_struct {
	int id;
	int num_clients;
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
	struct op ops[MAX_OPS];
	void *private;

	int sequence_point;

	/* Some functions need to be able to access arbitrary child
	 * structures from each child. */
	struct child_struct *all_children;
};

struct options {
	const char *backend;
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
	int clients_per_process;
	int one_byte_write_fix;
	int stat_check;
	int fake_io;
	int skip_cleanup;
	int per_client_results;
	const char *nfs;
	int nlm;
	const char *server;
	int run_once;
	int allow_scsi_writes;
	int trunc_io;
	const char *scsi_dev;
	const char *iscsi_device;
	const char *iscsi_initiatorname;
	int machine_readable;
	const char *smb_share;
	const char *smb_user;
	const char *block;
};


struct dbench_op {
	struct child_struct *child;
	const char *op;
	const char *fname;
	const char *fname2;
	const char *status;
	int64_t params[10];
};

struct backend_op {
	const char *name;
	void (*fn)(struct dbench_op *);
};

struct nb_operations {
	const char *backend_name;	
	struct backend_op *ops;
	int (*init)(void);
	void (*setup)(struct child_struct *child);
	void (*cleanup)(struct child_struct *child);
};
extern struct nb_operations *nb_ops;

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

struct nfsio;

extern struct options options;
extern int global_random;
#define RWBUFSIZE 16*1024*1024
extern char rw_buf[];

void all_string_sub(char *s,const char *pattern,const char *insert);
void child_run(struct child_struct *child0, const char *loadfile);
void msleep(unsigned int t);
int next_token(char **ptr,char *buff,char *sep);
int open_socket_in(int type, int port);
int open_socket_out(const char *host, int port);
int read_sock(int s, char *buf, int size);
void set_socket_options(int fd, char *options);
void *shm_setup(int size);
void single_string_sub(char *s,const char *pattern,const char *insert);
ssize_t sys_fgetxattr(int filedes, const char *name, void *value, size_t size);
int sys_fsetxattr(int filedes, const char *name, const void *value, size_t size, int flags);
ssize_t sys_getxattr(const char *path, const char *name, void *value, size_t size);
struct timeval timeval_current(void);
double timeval_elapsed(struct timeval *tv);
double timeval_elapsed2(struct timeval *tv1, struct timeval *tv2);
int write_sock(int s, char *buf, int size);
char *get_next_arg(const char *args, int id);

// copied from postgresql
#if defined(HAVE_FDATASYNC) && !HAVE_DECL_FDATASYNC
extern int  fdatasync(int fildes);
#endif
