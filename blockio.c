/* -*-  mode:c; tab-width:8; c-basic-offset:8; indent-tabs-mode:nil;  -*- */
/* 
   Copyright (C) by Ronnie Sahlberg <sahlberg@samba.org> 2008
   
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

#include "dbench.h"

#include <stdio.h>
#include <stdint.h>

#if defined(__APPLE__) & !HAVE_O_DIRECT
#include <fcntl.h>
#endif

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))
static void block_cleanup(struct child_struct *child)
{
	/* nothing to do */
}

static void block_setup(struct child_struct *child)
{
	int fd;
        
	child->rate.last_time = timeval_current();
	child->rate.last_bytes = 0;

	srandom(getpid() ^ time(NULL));

	child->private = calloc(1, sizeof(fd));
	fd = open(get_next_arg(options.block, 0), O_RDWR
#if defined(HAVE_O_DIRECT) 
                  |O_DIRECT
#endif
                  );
	if (fd < 0) {
		printf("Can not access block device %s\n", get_next_arg(options.block, 0));
		exit(10);
	}
#if defined(__APPLE__) && !HAVE_O_DIRECT
    fcntl(fd, F_NOCACHE);
#endif

    *(int *)child->private = fd;
}

static int check_status(int status, const char *expected)
{
	if (strcmp(expected, "*") == 0){
		return 1;
	}
	if (strncmp(expected, "0x", 2) == 0) {
		return status == strtol(expected, NULL, 16);
	}
	return 0;
}

static void failed(struct child_struct *child)
{
	child->failed = 1;
	printf("ERROR: child %d failed at line %d\n", child->id, child->line);
	exit(1);
}

static void block_fdatasync(struct dbench_op *op)
{
	int res = 0;

	res = fdatasync(*(int *)op->child->private);
	if (res < 0) {
		printf("[%d] FDATASYNC \"%s\" failed (%d)\n", 
		       op->child->line, op->fname,
		       res);
		failed(op->child);
	}
}

static void block_write(struct dbench_op *op)
{
	off_t offset = op->params[0];
	int len = op->params[1];
	int res = 0;
        uintptr_t aligned_buf = (uintptr_t)&rw_buf[4096] & ~0xfff;

	if ((options.trunc_io > 0) && (len > options.trunc_io)) {
		len = options.trunc_io;
	}
	if (len > RWBUFSIZE) {
		len = RWBUFSIZE;
	}

	res = pwrite(*(int *)op->child->private, (uint8_t *)aligned_buf, len, offset);
	if (res < 0) {
		printf("[%d] WRITE \"%s\" failed (%d)\n", 
		       op->child->line, op->fname,
		       res);
		failed(op->child);
	}
	op->child->bytes += len;
}

static void block_read(struct dbench_op *op)
{
	off_t offset = op->params[0];
	int len = op->params[1];
	int res = 0;
        uintptr_t aligned_buf = (uintptr_t)&rw_buf[4096] & ~0xfff;

	if ((options.trunc_io > 0) && (len > options.trunc_io)) {
		len = options.trunc_io;
	}
	if (len > RWBUFSIZE) {
		len = RWBUFSIZE;
	}

	res = pread(*(int *)op->child->private, (uint8_t *)aligned_buf, len, offset);
	if (res < 0) {
		printf("[%d] READ \"%s\" failed (%d)\n", 
		       op->child->line, op->fname,
		       res);
		failed(op->child);
	}
	op->child->bytes += len;
}

static int block_init(void)
{
        int fd;

        fd = open(get_next_arg(options.block, 0), O_RDWR);
        if (fd < 0) {
		printf("Can not access block device %s\n", get_next_arg(options.block, 0));
		return 1;
	}
        close(fd);

	return 0;
}

static struct backend_op ops[] = {
	{ "READ",    block_read },
	{ "WRITE",   block_write },
	{ "FDATASYNC",   block_fdatasync },
	{ NULL, NULL}
};

struct nb_operations block_ops = {
	.backend_name = "block",
	.init	      = block_init,
	.setup 	      = block_setup,
	.cleanup      = block_cleanup,
	.ops          = ops
};
