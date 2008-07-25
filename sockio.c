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

#include "dbench.h"

#define MAX_FILES 1000

struct sockio {
	char buf[70000];
	int sock;
};

/* emulate a single SMB packet exchange */
static void do_packets(struct child_struct *child, int send_size, int recv_size)
{
	struct sockio *sockio = (struct sockio *)child->private;
	uint32 *ubuf = (uint32 *)sockio->buf;

	ubuf[0] = htonl(send_size-4);
	ubuf[1] = htonl(recv_size-4);

	if (write_sock(sockio->sock, sockio->buf, send_size) != send_size) {
		printf("error writing %d bytes\n", (int)send_size);
		exit(1);
	}

	if (read_sock(sockio->sock, sockio->buf, 4) != 4) {
		printf("error reading header\n");
		exit(1);
	}

	if (ntohl(ubuf[0]) != (unsigned)(recv_size-4)) {
		printf("lost sync (%d %d)\n", 
		       (int)recv_size-4, (int)ntohl(ubuf[0]));
		exit(1);
	}

	if (recv(sockio->sock, sockio->buf, recv_size-4, MSG_WAITALL|MSG_TRUNC) != 
	    recv_size-4) {
		printf("error reading %d bytes\n", (int)recv_size-4);
		exit(1);
	}

	if (ntohl(ubuf[0]) != (unsigned)(recv_size-4)) {
		printf("lost sync (%d %d)\n", 
		       (int)recv_size-4, (int)ntohl(ubuf[0]));
	}
}


static void sio_setup(struct child_struct *child)
{
	struct sockio *sockio;
	sockio = calloc(1, sizeof(struct sockio));
	child->private = sockio;
	child->rate.last_time = timeval_current();
	child->rate.last_bytes = 0;
	
	sockio->sock = open_socket_out(options.server, TCP_PORT);
	if (sockio->sock == -1) {
		printf("client %d failed to start\n", child->id);
		exit(1);
	}
	set_socket_options(sockio->sock, options.tcp_options);

	do_packets(child, 8, 8);
}


static void sio_unlink(struct dbench_op *op)
{
        do_packets(op->child, 39+2+strlen(op->fname)*2+2, 39);
}

static void sio_mkdir(struct dbench_op *op)
{
        do_packets(op->child, 39+2+strlen(op->fname)*2+2, 39);
}

static void sio_rmdir(struct dbench_op *op)
{
        do_packets(op->child, 39+2+strlen(op->fname)*2+2, 39);
}

static void sio_createx(struct dbench_op *op)
{
        do_packets(op->child, 70+2+strlen(op->fname)*2+2, 39+12*4);
}

static void sio_writex(struct dbench_op *op)
{
	int size = op->params[2];
        do_packets(op->child, 39+20+size, 39+16);
	op->child->bytes += size;
}

static void sio_readx(struct dbench_op *op)
{
	int ret_size = op->params[3];
        do_packets(op->child, 39+20, 39+20+ret_size);
	op->child->bytes += ret_size;
}

static void sio_close(struct dbench_op *op)
{
        do_packets(op->child, 39+8, 39);
}

static void sio_rename(struct dbench_op *op)
{
	const char *old = op->fname;
	const char *new = op->fname2;
        do_packets(op->child, 39+8+2*strlen(old)+2*strlen(new), 39);
}

static void sio_flush(struct dbench_op *op)
{
        do_packets(op->child, 39+2, 39);
}

static void sio_qpathinfo(struct dbench_op *op)
{
        do_packets(op->child, 39+16+2*strlen(op->fname), 39+32);
}

static void sio_qfileinfo(struct dbench_op *op)
{
        do_packets(op->child, 39+20, 39+32);
}

static void sio_qfsinfo(struct dbench_op *op)
{
        do_packets(op->child, 39+20, 39+32);
}

static void sio_findfirst(struct dbench_op *op)
{
	int count = op->params[2];
        do_packets(op->child, 39+20+strlen(op->fname)*2, 39+90*count);
}

static void sio_cleanup(struct child_struct *child)
{
	(void)child;
}

static void sio_deltree(struct dbench_op *op)
{
	(void)op;
}

static void sio_sfileinfo(struct dbench_op *op)
{
        do_packets(op->child, 39+32, 39+8);
}

static void sio_lockx(struct dbench_op *op)
{
        do_packets(op->child, 39+12, 39);
}

static void sio_unlockx(struct dbench_op *op)
{
        do_packets(op->child, 39+12, 39);
}

static struct backend_op ops[] = {
	{ "Deltree", sio_deltree },
	{ "Flush", sio_flush },
	{ "Close", sio_close },
	{ "LockX", sio_lockx },
	{ "Rmdir", sio_rmdir },
	{ "Mkdir", sio_mkdir },
	{ "Rename", sio_rename },
	{ "ReadX", sio_readx },
	{ "WriteX", sio_writex },
	{ "Unlink", sio_unlink },
	{ "UnlockX", sio_unlockx },
	{ "FIND_FIRST", sio_findfirst },
	{ "SET_FILE_INFORMATION", sio_sfileinfo },
	{ "QUERY_FILE_INFORMATION", sio_qfileinfo },
	{ "QUERY_PATH_INFORMATION", sio_qpathinfo },
	{ "QUERY_FS_INFORMATION", sio_qfsinfo },
	{ "NTCreateX", sio_createx },
	{ NULL, NULL}
};

struct nb_operations sockio_ops = {
	.backend_name = "tbench",
	.setup 	      = sio_setup,
	.cleanup      = sio_cleanup,
	.ops          = ops
};
