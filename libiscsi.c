/* 
   Copyright (C) by Ronnie Sahlberg <ronniesahlberg@gmail.com> 2011
   
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

#if defined(HAVE_LIBISCSI)

#include "dbench.h"
#include <stdio.h>
#include <sys/types.h>
#include <stdint.h>

#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

static void iscsi_testunitready(struct dbench_op *op);
static void local_iscsi_readcapacity10(struct dbench_op *op, uint64_t *blocks);

static void iscsi_readcapacity10(struct dbench_op *op)
{
	return local_iscsi_readcapacity10(op, NULL);
}

struct iscsi_device {
       struct iscsi_context *iscsi;
       uint64_t blocks;
       int lun;
       char *initiator_name;
};


static void failed(struct child_struct *child)
{
	child->failed = 1;
	printf("ERROR: child %d failed at line %d\n", child->id, child->line);
	exit(1);
}

/* XXX merge with scsi.c */
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

static void iscsi_synchronizecache10(struct dbench_op *op)
{
	struct iscsi_device *sd;
	struct scsi_task *task;
	uint32_t lba = op->params[0];
	uint32_t xferlen = op->params[1];
	int syncnv = op->params[2];
	int immed = op->params[3];

	sd = op->child->private;

	if ((task = iscsi_synchronizecache10_sync(sd->iscsi, sd->lun, lba, xferlen, syncnv, immed)) == NULL) {
		printf("[%d] failed to send SYNCHRONIZECACHE10\n", op->child->line);
		failed(op->child);
		return;
	}
	if (!check_sense(task->status, op->status)) {
		if (task->status == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:0x%02x Ascq:0x%04x\n",
		       		    task->sense.key, task->sense.ascq);
	        }
		failed(op->child);
		scsi_free_scsi_task(task);
		return;
	}
	scsi_free_scsi_task(task);
}

static void iscsi_write10(struct dbench_op *op)
{
	struct iscsi_device *sd;
	struct scsi_task *task;
	uint32_t lba = op->params[0];
	uint32_t xferlen = op->params[1];	
	int fua = op->params[2];
	unsigned int data_size=1024*1024;
	char data[data_size];

	sd = op->child->private;

	lba = (lba / xferlen) * xferlen;

	/* make sure we wrap properly instead of failing if the loadfile
	   is bigger than our device
	*/
	if (sd->blocks <= lba) {
		lba = lba%sd->blocks;
	}
	if (sd->blocks <= lba+xferlen) {
		xferlen=1;
	}

	if ((task = iscsi_write10_sync(sd->iscsi, sd->lun, lba,
				       data, xferlen*512, 512,
				       0, 0, fua&0x04, fua&0x02, 0
	       )) == NULL) {
		printf("[%d] failed to send WRITE10\n", op->child->line);
		failed(op->child);
		return;
	}
	if (!check_sense(task->status, op->status)) {
		if (task->status == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:0x%02x Ascq:0x%04x\n",
		       		    task->sense.key, task->sense.ascq);
	        }
		failed(op->child);
		scsi_free_scsi_task(task);
		return;
	}

	op->child->bytes += xferlen*512;
	scsi_free_scsi_task(task);
}

static void iscsi_read10(struct dbench_op *op)
{
	struct iscsi_device *sd;
	struct scsi_task *task;
	uint32_t lba = op->params[0];
	uint32_t xferlen = op->params[1];	

	sd = op->child->private;

	lba = (lba / xferlen) * xferlen;

	/* make sure we wrap properly instead of failing if the loadfile
	   is bigger than our device
	*/
	if (sd->blocks <= lba) {
		lba = lba%sd->blocks;
	}
	if (sd->blocks <= lba+xferlen) {
		xferlen=1;
	}


	if ((task = iscsi_read10_sync(sd->iscsi, sd->lun, lba,
				      xferlen*512, 512,
				      0, 0, 0, 0, 0)) == NULL) {
		printf("[%d] failed to send READ10\n", op->child->line);
		failed(op->child);
		return;
	}
	if (!check_sense(task->status, op->status)) {
		if (task->status == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:0x%02x Ascq:0x%04x\n",
		       		    task->sense.key, task->sense.ascq);
	        }
		failed(op->child);
		scsi_free_scsi_task(task);
		return;
	}

	op->child->bytes += xferlen*512;
	scsi_free_scsi_task(task);
}


static void local_iscsi_readcapacity10(struct dbench_op *op, uint64_t *blocks)
{
	struct iscsi_device *sd;
	struct scsi_task *task;
	struct scsi_readcapacity10 *rc10;

	sd = op->child->private;

	if ((task = iscsi_readcapacity10_sync(sd->iscsi, sd->lun, op->params[0], op->params[1])) == NULL) {
		printf("[%d] failed to send READCAPACITY10\n", op->child->line);
		failed(op->child);
		return;
	}
	if (!check_sense(task->status, op->status)) {
		if (task->status == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:0x%02x Ascq:0x%04x\n",
		       		    task->sense.key, task->sense.ascq);
	        }
		failed(op->child);
		scsi_free_scsi_task(task);
		return;
	}

	rc10 = scsi_datain_unmarshall(task);
	if (rc10 == NULL) {
		printf("failed to unmarshall readcapacity10 data\n");
		failed(op->child);
		scsi_free_scsi_task(task);
		return;
	}

	if (blocks) {
		*blocks  = rc10->lba;
	}

	scsi_free_scsi_task(task);
}

static void iscsi_testunitready(struct dbench_op *op)
{
	struct iscsi_device *sd;
	struct scsi_task *task;

	sd = op->child->private;

	if ((task = iscsi_testunitready_sync(sd->iscsi, sd->lun)) == NULL) {
		printf("[%d] failed to send TESTUNITREADY\n", op->child->line);
		failed(op->child);
		return;
	}
	if (!check_sense(task->status, op->status)) {
		if (task->status == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:0x%02x Ascq:0x%04x\n",
		       		    task->sense.key, task->sense.ascq);
	        }
		failed(op->child);
		scsi_free_scsi_task(task);
		return;
	}
	scsi_free_scsi_task(task);
}


static void iscsi_cleanup(struct child_struct *child)
{
	struct iscsi_device *sd;

	sd=child->private;
	if (sd->iscsi != NULL) {
		iscsi_destroy_context(sd->iscsi);
	}
	free(sd);
}

static void iscsi_setup(struct child_struct *child)
{
	struct iscsi_device *sd;
	struct dbench_op fake_op;
	struct iscsi_url *iscsi_url = NULL;

	sd = malloc(sizeof(struct iscsi_device));
	if (sd == NULL) {
		printf("Failed to allocate iscsi device structure\n");
		return;
	}
	child->private=sd;
	child->id=99999;

	asprintf(&sd->initiator_name, "%s-%d", options.iscsi_initiatorname, child->id);

	sd->iscsi = iscsi_create_context(sd->initiator_name);
	if (sd->iscsi == NULL) {
		printf("Failed to create context\n");
		return;
	}

	iscsi_url = iscsi_parse_full_url(sd->iscsi, options.iscsi_device);
	if (iscsi_url == NULL) {
		fprintf(stderr, "Failed to parse URL: %s\n", 
			iscsi_get_error(sd->iscsi));
		return;
	}

	sd->lun = iscsi_url->lun;

	iscsi_set_targetname(sd->iscsi, iscsi_url->target);
	iscsi_set_session_type(sd->iscsi, ISCSI_SESSION_NORMAL);
	iscsi_set_header_digest(sd->iscsi, ISCSI_HEADER_DIGEST_NONE_CRC32C);

	if (iscsi_url->user != NULL) {
		if (iscsi_set_initiator_username_pwd(sd->iscsi, iscsi_url->user, iscsi_url->passwd) != 0) {
			fprintf(stderr, "Failed to set initiator username and password\n");
			return;
		}
	}

	if (iscsi_full_connect_sync(sd->iscsi, iscsi_url->portal, iscsi_url->lun) != 0) {
		fprintf(stderr, "Login Failed. %s\n", iscsi_get_error(sd->iscsi));
		iscsi_destroy_url(iscsi_url);
		iscsi_destroy_context(sd->iscsi);
		return;
	}

	fake_op.child=child;
	fake_op.status="*";
	iscsi_testunitready(&fake_op);

	fake_op.params[0]=0;
	fake_op.params[1]=0;
	fake_op.status="*";
	local_iscsi_readcapacity10(&fake_op, &sd->blocks);
}

static int iscsi_init(void)
{
	struct iscsi_device *sd;
	struct dbench_op fake_op;
	struct child_struct child;
	struct iscsi_url *iscsi_url = NULL;

	sd = malloc(sizeof(struct iscsi_device));
	if (sd == NULL) {
		printf("Failed to allocate iscsi device structure\n");
		return -1;
	}
	child.private=sd;
	child.id=99999;

	asprintf(&sd->initiator_name, "%s-%d", options.iscsi_initiatorname, child.id);

	sd->iscsi = iscsi_create_context(sd->initiator_name);
	if (sd->iscsi == NULL) {
		printf("Failed to create context\n");
		return -1;
	}

	iscsi_url = iscsi_parse_full_url(sd->iscsi, options.iscsi_device);
	if (iscsi_url == NULL) {
		fprintf(stderr, "Failed to parse URL: %s\n", 
			iscsi_get_error(sd->iscsi));
		return -1;
	}

	sd->lun = iscsi_url->lun;

	iscsi_set_targetname(sd->iscsi, iscsi_url->target);
	iscsi_set_session_type(sd->iscsi, ISCSI_SESSION_NORMAL);
	iscsi_set_header_digest(sd->iscsi, ISCSI_HEADER_DIGEST_NONE_CRC32C);

	if (iscsi_url->user != NULL) {
		if (iscsi_set_initiator_username_pwd(sd->iscsi, iscsi_url->user, iscsi_url->passwd) != 0) {
			fprintf(stderr, "Failed to set initiator username and password\n");
			return -1;
		}
	}

	if (iscsi_full_connect_sync(sd->iscsi, iscsi_url->portal, iscsi_url->lun) != 0) {
		fprintf(stderr, "Login Failed. %s\n", iscsi_get_error(sd->iscsi));
		iscsi_destroy_url(iscsi_url);
		iscsi_destroy_context(sd->iscsi);
		return -1;
	}

	fake_op.child=&child;
	fake_op.status="*";
	iscsi_testunitready(&fake_op);

	fake_op.params[0]=0;
	fake_op.params[1]=0;
	fake_op.status="*";
	local_iscsi_readcapacity10(&fake_op, &sd->blocks);

	free(sd);

	return 0;
}


static struct backend_op ops[] = {
	{ "TESTUNITREADY",      iscsi_testunitready },
	{ "READ10",             iscsi_read10 },
	{ "READCAPACITY10",     iscsi_readcapacity10 },
	{ "SYNCHRONIZECACHE10", iscsi_synchronizecache10 },
	{ "WRITE10",            iscsi_write10 },
	{ NULL, NULL}
};

struct nb_operations iscsi_ops = {
	.backend_name = "iscsibench",
	.init	      = iscsi_init,
	.setup 	      = iscsi_setup,
	.cleanup      = iscsi_cleanup,
	.ops          = ops
};

#endif

