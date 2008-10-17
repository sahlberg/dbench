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

#define _GNU_SOURCE
#include <stdio.h>
#undef _GNU_SOURCE

#include "dbench.h"

#ifdef HAVE_LINUX_SCSI_SG

#include <fcntl.h>
#include <sys/ioctl.h>
#include <scsi/sg.h>

#define SCSI_TIMEOUT 5000 /* ms */

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

struct scsi_device {
       int fd;
};

static void scsi_setup(struct child_struct *child)
{
	int vers;
	struct scsi_device *sd;

	sd = malloc(sizeof(struct scsi_device));
	if (sd == NULL) {
		printf("Failed to allocate scsi device structure\n");
			exit(10);
	}
	child->private=sd;
	if((sd->fd=open(options.scsi_dev, O_RDWR))<0){
		printf("Failed to open scsi device node : %s\n", options.scsi_dev);
		free(sd);
		exit(10);
	}
	if ((ioctl(sd->fd, SG_GET_VERSION_NUM, &vers) < 0) || (vers < 30000)) {
		printf("%s is not a SCSI device node\n", options.scsi_dev);
		close(sd->fd);
		free(sd);
		exit(10);
	}
}

static void scsi_cleanup(struct child_struct *child)
{
	struct scsi_device *sd;

	sd=child->private;
	close(sd->fd);
	sd->fd=-1;
	free(sd);
}


static int scsi_io(int fd, unsigned char *cdb, unsigned char cdb_size, int xfer_dir, unsigned int *data_size, char *data, unsigned char *sc)
{
	sg_io_hdr_t io_hdr;
	unsigned int sense_len=32;
	unsigned char sense[sense_len];

	*sc = 0;

	memset(&io_hdr, 0, sizeof(sg_io_hdr_t));
	io_hdr.interface_id = 'S';

	/* CDB */
	io_hdr.cmdp = cdb;
	io_hdr.cmd_len = cdb_size;

	/* Where to store the sense_data, if there was an error */
	io_hdr.sbp = sense;
	io_hdr.mx_sb_len = sense_len;
	sense_len=0;

	/* Transfer direction, either in or out. Linux does not yet
	   support bidirectional SCSI transfers ?
	 */
	io_hdr.dxfer_direction = xfer_dir;

	/* Where to store the DATA IN/OUT from the device and how big the
	   buffer is
	 */
	io_hdr.dxferp = data;
	io_hdr.dxfer_len = *data_size;

	/* SCSI timeout in ms */
	io_hdr.timeout = SCSI_TIMEOUT;


	if(ioctl(fd, SG_IO, &io_hdr) < 0){
		perror("SG_IO ioctl failed");
		return -1;
	}

	/* now for the error processing */
	if((io_hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK){
		if(io_hdr.sb_len_wr > 0){
			sense_len=io_hdr.sb_len_wr;
			*sc=sense[2]&0x0f;
			return 0;
		}
	}
	if(io_hdr.masked_status){
		printf("SCSI status=0x%x\n", io_hdr.status);
		printf("SCSI masked_status=0x%x\n", io_hdr.masked_status);
		return -2;
	}
	if(io_hdr.host_status){
		printf("SCSI host_status=0x%x\n", io_hdr.host_status);
		return -3;
	}
	if(io_hdr.driver_status){
		printf("driver_status=0x%x\n", io_hdr.driver_status);
		return -4;
	}

	return 0;
}


static int check_sense(unsigned char sc, const char *expected)
{
	if (strcmp(expected, "*") == 0){
		return 1;
	}
	if (strncmp(expected, "0x", 2) == 0) {
		return sc == strtol(expected, NULL, 16);
	}
	return 0;
}

static void failed(struct child_struct *child)
{
	child->failed = 1;
	printf("ERROR: child %d failed at line %d\n", child->id, child->line);
	exit(1);
}

static void scsi_testunitready(struct dbench_op *op)
{
	struct scsi_device *sd;
	unsigned char cdb[]={0,0,0,0,0,0};
	int res;
	unsigned char sc;
	unsigned int data_size=200;
	char data[data_size];

	sd = op->child->private;

	res=scsi_io(sd->fd, cdb, sizeof(cdb), SG_DXFER_FROM_DEV, &data_size, data, &sc);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] TESTUNITREADY \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		failed(op->child);
	}

	return;
}


static void scsi_read10(struct dbench_op *op)
{
	struct scsi_device *sd;
	unsigned char cdb[]={0x28,0,0,0,0,0,0,0,0,0};
	int res;
	int lba = op->params[0];
	int xferlen = op->params[1];
	int rd = op->params[2];
	int grp = op->params[3];
	unsigned int data_size=1024*1024;
	char data[data_size];
	unsigned char sc;

	cdb[1] = rd;

	cdb[2] = (lba>>24)&0xff;
	cdb[3] = (lba>>16)&0xff;
	cdb[4] = (lba>> 8)&0xff;
	cdb[5] = (lba    )&0xff;

	cdb[6] = grp&0x1f;

	cdb[7] = (xferlen>>8)&0xff;
	cdb[8] = xferlen&0xff;
	data_size = xferlen*512;

	sd = op->child->private;

	res=scsi_io(sd->fd, cdb, sizeof(cdb), SG_DXFER_FROM_DEV, &data_size, data, &sc);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] READ10 \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		failed(op->child);
	}

	op->child->bytes += xferlen*512;
}

static void scsi_readcapacity10(struct dbench_op *op)
{
	struct scsi_device *sd;
	unsigned char cdb[]={0x25,0,0,0,0,0,0,0,0,0};
	int res;
	int lba = op->params[0];
	int pmi = op->params[1];
	unsigned int data_size=8;
	char data[data_size];
	unsigned char sc;

	cdb[2] = (lba>>24)&0xff;
	cdb[3] = (lba>>16)&0xff;
	cdb[4] = (lba>> 8)&0xff;
	cdb[5] = (lba    )&0xff;

	cdb[8] = (pmi?1:0);

	sd = op->child->private;

	res=scsi_io(sd->fd, cdb, sizeof(cdb), SG_DXFER_FROM_DEV, &data_size, data, &sc);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] READCAPACITY10 \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		failed(op->child);
	}
}

static struct backend_op ops[] = {
	{ "READ10",           scsi_read10 },
	{ "READCAPACITY10",   scsi_readcapacity10 },
	{ "TESTUNITREADY",    scsi_testunitready },
	{ NULL, NULL}
};

struct nb_operations scsi_ops = {
	.backend_name = "scsibench",
	.setup 	      = scsi_setup,
	.cleanup      = scsi_cleanup,
	.ops          = ops
};

#endif /* HAVE_LINUX_SCSI_SG */
