/* 
   Copyright (C) by Ronnie Sahlberg <ronniesahlberg@gmail.com> 2009
   
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

#if !defined(HAVE_LIBISCSI)

#include "dbench.h"
#include <stdio.h>
#include <sys/types.h>
#include <stdint.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

static void iscsi_testunitready(struct dbench_op *op);
static void local_iscsi_readcapacity10(struct dbench_op *op, uint64_t *blocks);

static void iscsi_readcapacity10(struct dbench_op *op)
{
	return local_iscsi_readcapacity10(op, NULL);
}

struct iscsi_device {
       const char *portal;
       int port;
       const char *target;

       int s;
       uint64_t isid;
       uint32_t itt;
       uint32_t cmd_sn;
       uint32_t exp_stat_sn;
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


#ifndef SG_DXFER_NONE
#define SG_DXFER_NONE -1
#endif
#ifndef SG_DXFER_TO_DEV
#define SG_DXFER_TO_DEV -2
#endif
#ifndef SG_DXFER_FROM_DEV
#define SG_DXFER_FROM_DEV -3
#endif
#define PARAMETERS_SIZE 24
#define PROUT_CMD 0x5F
#define PROUT_SCOPE_LU_SCOPE 0x0

#define SCSI_STATUS_GOOD			0x00
#define SCSI_STATUS_CHECK_CONDITION		0x02
#define SCSI_STATUS_BUSY			0x08
#define SCSI_STATUS_RESERVATION_CONFLICT	0x18
#define SCSI_STATUS_TASK_SET_FULL		0x28
#define SCSI_STATUS_ACA_ACTIVE			0x30
#define SCSI_STATUS_TASK_ABORTED		0x40

struct scsi_status_name {
       int sc;
       const char *name;
};
static struct scsi_status_name scsi_status_names[] = {
     { 0x00, "SCSI_STATUS_GOOD" },
     { 0x02, "SCSI_STATUS_CHECK_CONDITION" },
     { 0x08, "SCSI_STATUS_BUSY" },
     { 0x18, "SCSI_STATUS_RESERVATION_CONFLICT" },
     { 0x28, "SCSI_STATUS_TASK_SET_FULL" },
     { 0x30, "SCSI_STATUS_ACA_ACTIVE" },
     { 0x40, "SCSI_STATUS_TASK_ABORTED" },
     { 0x00, NULL }
};
static struct scsi_status_name scsi_key_names[] = {
     { 0x05, "ILLEGAL_REQUEST" },
     { 0x06, "UNIT_ATTENTION" },
     { 0x00, NULL }
};
static struct scsi_status_name scsi_ascq_names[] = {
     { 0x2000, "INVALID COMMAND OPERATION CODE" },
     { 0x2900, "POWER ON RESET" },
     { 0x00, NULL }
};

const char *scsi_status_name(int sc, struct scsi_status_name *names) {
      struct scsi_status_name *sn = names;

      while (sn->name != NULL) {
      	    if (sn->sc == sc)
	    	    return sn->name;
      	    sn++;
      }
      return "unknown";   
};


void set_nonblocking(int fd)
{
	unsigned v;
	v = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, v | O_NONBLOCK);
}


struct login_param {
       struct login_param *next;
       char *arg;
       char *value;
};
struct login_param *login_params;

static void add_login_param(char *arg, char *value)
{
	struct login_param *new_param;

	new_param = malloc(sizeof(struct login_param));
	if (new_param == NULL) {
		printf("Failed to allocate login param struct\n");
		exit(10);
	}
	new_param->arg   = strdup(arg);
	new_param->value = strdup(value);
	new_param->next  = login_params;
	login_params     = new_param;
}

static int send_iscsi_pdu(struct iscsi_device *sd, char *ish, char *data, int len)
{
	char *buf, *ptr;
	ssize_t remaining, count;

	/* itt */
	ish[16] = (sd->itt>>24)&0xff;
	ish[17] = (sd->itt>>16)&0xff;
	ish[18] = (sd->itt>> 8)&0xff;
	ish[19] = (sd->itt    )&0xff;

	/* command sequence number */
	ish[24]  = (sd->cmd_sn>>24)&0xff;
	ish[25]  = (sd->cmd_sn>>16)&0xff;
	ish[26]  = (sd->cmd_sn>> 8)&0xff;
	ish[27]  = (sd->cmd_sn    )&0xff;

	/* expected stat sequence number */
	ish[28]  = (sd->exp_stat_sn>>24)&0xff;
	ish[29]  = (sd->exp_stat_sn>>16)&0xff;
	ish[30]  = (sd->exp_stat_sn>> 8)&0xff;
	ish[31]  = (sd->exp_stat_sn    )&0xff;

	buf=malloc(48+len+4);
	if (buf == NULL) {
		printf("Failed to allocate buffer for PDU of size %d bytes\n", 48+len);
		return -1;
	}

	memcpy(buf, ish, 48);
	if (len > 0) {
		memcpy(buf+48, data, len);
	}
	remaining = 48 + len;
	remaining = (remaining+3)&0xfffffc;
	ptr = buf;
	while(remaining > 0) {
		count = write(sd->s, ptr, remaining);
		if (count == -1) {
			printf("Write to socket failed with errno %d(%s)\n", errno, strerror(errno));
			free(buf);
			return -1;
		}
		remaining-= count;
	}

	free(buf);
	return 0;
}

static int wait_for_pdu(struct iscsi_device *sd, char *ish, char *data, unsigned int *data_size, char *sense_data)
{
	char *buf, *ptr;
	ssize_t total, remaining, count;
	unsigned int itt, dsl;
	uint32_t ecsn, ssn;

	remaining = 48;
	ptr = ish;
	while (remaining > 0) {
		count = read(sd->s, ptr, remaining);
		if (count == -1) {
			printf("Read from socket failed with errno %d(%s)\n", errno, strerror(errno));
			return -1;
		}
		remaining-= count;
		ptr += count;
	}

	/* verify the itt */
	itt  = (ish[16]&0xff)<<24;
	itt |= (ish[17]&0xff)<<16;
	itt |= (ish[18]&0xff)<< 8;
	itt |= (ish[19]&0xff);
	if (itt != sd->itt) {
		printf("Wrong ITT in PDU. Expected 0x%08x, got 0x%08x\n", sd->itt, itt);
		exit(10);
	} 

	/* data segment length */
	dsl  = (ish[5]&0xff)<<16;
	dsl |= (ish[6]&0xff)<<8;
	dsl |= (ish[7]&0xff);

	total = (dsl+3)&0xfffffffc;
	remaining = total;
	buf = malloc(remaining);
	if (buf == NULL) {
		printf("Failed to alloc buf to read data into\n");
		return -1;
	}
	ptr = buf;
	while (remaining > 0) {
		count = read(sd->s, ptr, remaining);

		if (count == -1) {
			printf("Read from socket failed with errno %d(%s)\n", errno, strerror(errno));
			return -1;
		}
		remaining-= count;
		ptr += count;
	}

	/* stat sequence number */
	ssn  = (ish[24]&0xff)<<24;
	ssn |= (ish[25]&0xff)<<16;
	ssn |= (ish[26]&0xff)<<8;
	ssn |= (ish[27]&0xff);
	sd->exp_stat_sn = ssn+1;

	/* expected command sequence number */
	ecsn  = (ish[28]&0xff)<<24;
	ecsn |= (ish[29]&0xff)<<16;
	ecsn |= (ish[30]&0xff)<<8;
	ecsn |= (ish[31]&0xff);
	sd->cmd_sn = ecsn;

	if (ish[0]&0x3f) {
		unsigned long int buffer_offset;

		buffer_offset  = (ish[40]&0xff)<<24;
		buffer_offset |= (ish[41]&0xff)<<16;
		buffer_offset |= (ish[42]&0xff)<<8;
		buffer_offset |= (ish[43]&0xff);

		/* scsi response with check condition and sense data*/
		if ((ish[0]&0x3f) == 33 && ish[2] == 0 && ish[3] == 2 )  {
			if (sense_data) {
				memcpy(sense_data, buf, 32);
			}
		}

		if (buffer_offset == 0) {
			/* we only return the data from the first data-in pdu */
			if (data_size && *data_size > 0) {
				if ((ssize_t)*data_size > total) {
					*data_size = total;
				}
				if (data) {
					memcpy(data, buf, *data_size);
				}
			}
		}
	}

	free(buf);
	return 0;
}

static int iscsi_login(struct iscsi_device *sd)
{
	char ish[48];
	int len;
	struct login_param *login_param;
	char *data, *ptr;

	add_login_param("SessionType", "Normal");
	add_login_param("HeaderDigest", "None");
	add_login_param("DataDigest", "None");
	add_login_param("DefaultTime2Wait", "0");
	add_login_param("DefaultTime2Retain", "0");
	add_login_param("InitialR2T", "Yes");
	add_login_param("ImmediateData", "Yes");
	add_login_param("MaxBurstLength", "16776192");
	add_login_param("FirstBurstLength", "16776192");
	add_login_param("MaxOutstandingR2T", "1");
	add_login_param("MaxRecvDataSegmentLength", "16776192");
	add_login_param("DataPDUInOrder", "Yes");
	add_login_param("MaxConnections", "1");
	add_login_param("TargetName", discard_const(sd->target));
	add_login_param("InitiatorName", sd->initiator_name);

	bzero(ish, 48);
	/* opcode : LOGIN REQUEST (I) */
	ish[0] = 0x43;

	/* T CSG:op NSG:full feature */
	ish[1] = 0x87;

	/* data segment length */
	for(login_param=login_params, len=0; login_param; login_param=login_param->next) {
		len += strlen(login_param->arg);
		len += 1;
		len += strlen(login_param->value);
		len += 1;
	}
	/* data segment length */
	ish[5] = (len>>16)&0xff;
	ish[6] = (len>> 8)&0xff;
	ish[7] = (len    )&0xff;


	/* isid */
	ish[8] = (sd->isid>>40)&0xff;
	ish[9] = (sd->isid>>32)&0xff;
	ish[10] = (sd->isid>>24)&0xff;
	ish[11] = (sd->isid>>16)&0xff;
	ish[12] = (sd->isid>> 8)&0xff;
	ish[13] = (sd->isid    )&0xff;
	
	data = malloc(len);
	for(login_param=login_params, ptr=data; login_param; login_param=login_param->next) {
		strcpy(ptr,login_param->arg);
		ptr+=strlen(login_param->arg);
		*ptr='=';
		ptr++;
		strcpy(ptr,login_param->value);
		ptr+=strlen(login_param->value);
		*ptr=0;
		ptr++;
	}

	if (send_iscsi_pdu(sd, ish, data, len) != 0) {
	   	printf("Failed to send iscsi pdu\n");
		return -1;
	}

	if (wait_for_pdu(sd, ish, NULL, NULL, NULL) != 0) {
	   	printf("Failed to send iscsi pdu\n");
		return -1;
	}



	free(data);
	return 0;
}


static int do_iscsi_io(struct iscsi_device *sd, unsigned char *cdb, unsigned char cdb_size, int xfer_dir, unsigned int *data_size, char *data, unsigned char *sc, int *sense_key, int *sense_ascq)
{
	char ish[48];
	char sense_data[48];
	int data_in_len=0, data_out_len=0;

	bzero(ish, 48);

	/* opcode : SCSI command */
	ish[0] = 0x01;

	/* flags */
	ish[1] = 0x81; /* F + SIMPLE */
	if (xfer_dir == SG_DXFER_FROM_DEV) {
		ish[1] |= 0x40;
		data_in_len= *data_size;
		data_out_len=0;
	}
	if (xfer_dir == SG_DXFER_TO_DEV) {
		ish[1] |= 0x20;

		/* data segment length */
		ish[5] = ((*data_size)>>16)&0xff;
		ish[6] = ((*data_size)>> 8)&0xff;
		ish[7] = ((*data_size)    )&0xff;

		data_in_len=0;
		data_out_len=*data_size;
	}

	/* lun */
	ish[9] = sd->lun;

	/* expected data xfer len */
	ish[20]  = ((*data_size)>>24)&0xff;
	ish[21]  = ((*data_size)>>16)&0xff;
	ish[22]  = ((*data_size)>> 8)&0xff;
	ish[23]  = ((*data_size)    )&0xff;

	/* cdb */
	memcpy(ish+32, cdb, cdb_size);

	*data_size=data_out_len;
	if (send_iscsi_pdu(sd, ish, data, *data_size) != 0) {
	   	printf("Failed to send iscsi pdu\n");
		return -1;
	}

need_more_data:
	*data_size=data_in_len;
	if (wait_for_pdu(sd, ish, data, data_size, sense_data) != 0) {
	   	printf("Failed to receive iscsi pdu\n");
		return -1;
	}

	switch (ish[0]&0x3f) {
	case 0x21: /* SCSI response */
		if (ish[2] != 0) {
			printf("SCSI Response %d\n", ish[2]);
			sd->itt++;
			return -1;
		}
		*sc = ish[3];
		if (*sc == SCSI_STATUS_CHECK_CONDITION) {
		   *sense_key = sense_data[4];
		   *sense_ascq = sense_data[14] << 8 | sense_data[15];
	        }
		return 0;
		break;
	case 0x25: /* SCSI Data-In */
		if (ish[1]&0x01) {
			*sc = ish[3];
			sd->itt++;
			return 0;
		}
		/* no sbit, it means there is more data to read */
		goto need_more_data;
		break;
	default:
		printf("got unsupported PDU:0x%02x\n", ish[0]&0x3f);
	}

	*sc = 0;
	sd->itt++;
	return 0;
}

static void iscsi_read10(struct dbench_op *op)
{
	struct iscsi_device *sd=op->child->private;
	unsigned char cdb[]={0x28,0,0,0,0,0,0,0,0,0};
	int res;
	uint32_t lba = op->params[0];
	uint32_t xferlen = op->params[1];
	int rd = op->params[2];
	int grp = op->params[3];
	unsigned int data_size=1024*1024;
	char data[data_size];
	unsigned char sc;
	int sense_key, sense_ascq;

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

	cdb[1] = rd;

	cdb[2] = (lba>>24)&0xff;
	cdb[3] = (lba>>16)&0xff;
	cdb[4] = (lba>> 8)&0xff;
	cdb[5] = (lba    )&0xff;

	cdb[6] = grp&0x1f;

	cdb[7] = (xferlen>>8)&0xff;
	cdb[8] = xferlen&0xff;
	data_size = xferlen*512;

	res=do_iscsi_io(sd, cdb, sizeof(cdb), SG_DXFER_FROM_DEV, &data_size, data, &sc, &sense_key, &sense_ascq);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] READ10 \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		if (sc == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:%s(%d) Ascq:%s(0x%04x)\n",
		       	     scsi_status_name(sense_key, &scsi_key_names[0]), sense_key, scsi_status_name(sense_ascq, &scsi_ascq_names[0]), sense_ascq);
	        }
		failed(op->child);
	}

	op->child->bytes += xferlen*512;
}

static void iscsi_synchronizecache10(struct dbench_op *op)
{
	struct iscsi_device *sd;
	unsigned char cdb[]={0x35,0,0,0,0,0,0,0,0,0};
	int res;
	uint32_t lba = op->params[0];
	uint32_t xferlen = op->params[1];
	int syncnv = op->params[2];
	int immed = op->params[3];
	unsigned char sc;
	int sense_key, sense_ascq;
	unsigned int data_size=0;

	sd = op->child->private;

	if (syncnv) {
		cdb[1] |= 0x04;
	}
	if (immed) {
		cdb[1] |= 0x02;
	}
	cdb[2] = (lba>>24)&0xff;
	cdb[3] = (lba>>16)&0xff;
	cdb[4] = (lba>> 8)&0xff;
	cdb[5] = (lba    )&0xff;

	cdb[7] = (xferlen>>8)&0xff;
	cdb[8] = xferlen&0xff;

	res=do_iscsi_io(sd, cdb, sizeof(cdb), SG_DXFER_NONE, &data_size, NULL, &sc, &sense_key, &sense_ascq);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] SYNCHRONIZECACHE10 \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		if (sc == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:%s(%d) Ascq:%s(0x%04x)\n",
		       	     scsi_status_name(sense_key, &scsi_key_names[0]), sense_key, scsi_status_name(sense_ascq, &scsi_ascq_names[0]), sense_ascq);
	        }
		failed(op->child);
	}
	return;
}

static void iscsi_write10(struct dbench_op *op)
{
	struct iscsi_device *sd=op->child->private;
	unsigned char cdb[]={0x2a,0,0,0,0,0,0,0,0,0};
	int res;
	uint32_t lba = op->params[0];
	uint32_t xferlen = op->params[1];
	int fua = op->params[2];
	int grp = op->params[3];
	unsigned int data_size=1024*1024;
	char data[data_size];
	unsigned char sc;
	int sense_key, sense_ascq;

	if (!options.allow_scsi_writes) {
		printf("WRITE10 command in loadfile but --allow-scsi-writes not specified. Aborting.\n");
		failed(op->child);
	}

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

	cdb[1] = fua;

	cdb[2] = (lba>>24)&0xff;
	cdb[3] = (lba>>16)&0xff;
	cdb[4] = (lba>> 8)&0xff;
	cdb[5] = (lba    )&0xff;

	cdb[6] = grp&0x1f;

	cdb[7] = (xferlen>>8)&0xff;
	cdb[8] = xferlen&0xff;
	data_size = xferlen*512;

	res=do_iscsi_io(sd, cdb, sizeof(cdb), SG_DXFER_TO_DEV, &data_size, data, &sc, &sense_key, &sense_ascq);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] READ10 \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		if (sc == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:%s(%d) Ascq:%s(0x%04x)\n",
		       	     scsi_status_name(sense_key, &scsi_key_names[0]), sense_key, scsi_status_name(sense_ascq, &scsi_ascq_names[0]), sense_ascq);
	        }
		failed(op->child);
	}

	op->child->bytes += xferlen*512;
}


/* <service action> <type> <key> <sa-key>*/
static void iscsi_prout(struct dbench_op *op)
{
	struct iscsi_device *sd;
	unsigned char sc;
	int sense_key, sense_ascq;
	unsigned char cdb[10];
	char parameters[PARAMETERS_SIZE];
	int i;
	unsigned int data_size = PARAMETERS_SIZE;
	u_int64_t sa, type, key, sakey;

	sa = op->params[0];
	type = op->params[1];
	key = op->params[2];
	sakey = op->params[3];

	bzero(parameters, PARAMETERS_SIZE);
	bzero(cdb, 10);

	/* Persistent Reserve OUT */
	cdb[0] = PROUT_CMD;
	/* Registering a key */
	cdb[1] |= sa & 0x1f;

	cdb[2] |= (PROUT_SCOPE_LU_SCOPE<<4) & 0xf0;
	cdb[2] |= type & 0x0f;

	/* Parameters size */
	cdb[8] = PARAMETERS_SIZE;

	/* splitting 64 bits in 8 blocks of 8 */
	for (i = 0; i <= 7; i++) {
		parameters[i] = (char) ((key >> 56) & 0xff);
		key <<= 8;

		parameters[i+8] = (char) ((sakey >> 56) & 0xff);
		sakey <<= 8;
	}

	sd = op->child->private;

	i = do_iscsi_io(sd, cdb, sizeof(cdb), SG_DXFER_TO_DEV, &data_size,
	                   parameters, &sc, &sense_key, &sense_ascq);
	if (i) {
		printf("SCSI_IO failed\n");
		failed(op->child);
	}


	if (!check_sense(sc, op->status)) {
		printf("[%d] PROUT \"%s\" failed with %s(0x%02x) - expected %s\n",
			op->child->line, op->fname, scsi_status_name(sc, &scsi_status_names[0]), sc, op->status);
		if (sc == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:%s(%d) Ascq:%s(0x%04x)\n",
		       	     scsi_status_name(sense_key, &scsi_key_names[0]), sense_key, scsi_status_name(sense_ascq, &scsi_ascq_names[0]), sense_ascq);
	        }
		failed(op->child);
	}
}

static void iscsi_setup(struct child_struct *child)
{
	struct iscsi_device *sd;
	struct sockaddr_in sin;
	struct dbench_op fake_op;
	char *portal;
	char *port;
	char *target;
	char *lun;

	sd = malloc(sizeof(struct iscsi_device));
	if (sd == NULL) {
		printf("Failed to allocate iscsi device structure\n");
		exit(10);
	}
	child->private=sd;

	if (options.iscsi_device == NULL) {
		printf("Must specify iSCSI URL for the target device\n");
		return;
	}
	if (strncmp(options.iscsi_device, "iscsi://", 8)) {
		printf("Invalid iSCSI device URL. Syntax is \"iscsi://<ip-address>[:<port>]/<target-iqn>/<lun>\"\n");
		return;
	}
	portal = strdup(&options.iscsi_device[8]);
	target = strchr(portal, '/');
	if (target == NULL) {
		printf("Invalid iSCSI device URL. Syntax is \"iscsi://<ip-address>[:<port>]/<target-iqn>/<lun>\"\n");
		return;
	}
	*target++ = '\0';
	lun = strchr(target, '/');
	if (lun == NULL) {
		printf("Invalid iSCSI device URL. Syntax is \"iscsi://<ip-address>[:<port>]/<target-iqn>/<lun>\"\n");
		return;
	}
	*lun++ = '\0';
	port = strchr(portal, ':');
	if (port != NULL) {
		*port++ = '\0';
	}

	sd->portal = portal;
	sd->target = target;
	sd->port   = port?atoi(port):3260;
	sd->lun    = atoi(lun);
	asprintf(&sd->initiator_name, "%s-%d", options.iscsi_initiatorname, child->id);

	sd->isid  =0x0000800000000000ULL | child->id; 
	sd->s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sd->s == -1) {
		printf("could not open socket() errno:%d(%s)\n", errno, strerror(errno));
		exit(10);
	}

	sin.sin_family      = AF_INET;
	sin.sin_port        = htons(sd->port);
	if (inet_pton(AF_INET, sd->portal, &sin.sin_addr) != 1) {
		printf("Failed to convert \"%s\" into an address\n", sd->portal);
		exit(10);
	}

	if (connect(sd->s, (struct sockaddr *)&sin, sizeof(sin)) != 0) {
		printf("connect failed with errno:%d(%s)\n", errno, strerror(errno));
		exit(10);
	}

	sd->itt=0x000a0000;
	sd->cmd_sn=0;
	sd->exp_stat_sn=0;
	if (iscsi_login(sd) != 0) {
		printf("Failed to log in to target.\n");
		exit(10);
	}

	fake_op.child=child;
	fake_op.status="*";
	iscsi_testunitready(&fake_op);

	fake_op.params[0]=0;
	fake_op.params[1]=0;
	fake_op.status="*";
	local_iscsi_readcapacity10(&fake_op, &sd->blocks);
}

       
static void iscsi_cleanup(struct child_struct *child)
{
	struct iscsi_device *sd;

	sd=child->private;
	close(sd->s);
	sd->s=-1;
	free(sd);
}

static int iscsi_init(void)
{
	struct iscsi_device *sd;
	struct sockaddr_in sin;
	struct dbench_op fake_op;
	struct child_struct child;
	char *portal;
	char *port;
	char *target;
	char *lun;

	sd = malloc(sizeof(struct iscsi_device));
	if (sd == NULL) {
		printf("Failed to allocate iscsi device structure\n");
		return 1;
	}
	child.private=sd;
	child.id=99999;


	if (options.iscsi_device == NULL) {
		printf("Must specify iSCSI URL for the target device\n");
		return -1;
	}
	if (strncmp(options.iscsi_device, "iscsi://", 8)) {
		printf("Invalid iSCSI device URL. Syntax is \"iscsi://<ip-address>[:<port>]/<target-iqn>/<lun>\"\n");
		return -1;
	}
	portal = strdup(&options.iscsi_device[8]);
	target = strchr(portal, '/');
	if (target == NULL) {
		printf("Invalid iSCSI device URL. Syntax is \"iscsi://<ip-address>[:<port>]/<target-iqn>/<lun>\"\n");
		return -1;
	}
	*target++ = '\0';
	lun = strchr(target, '/');
	if (lun == NULL) {
		printf("Invalid iSCSI device URL. Syntax is \"iscsi://<ip-address>[:<port>]/<target-iqn>/<lun>\"\n");
		return -1;
	}
	*lun++ = '\0';
	port = strchr(portal, ':');
	if (port != NULL) {
		*port++ = '\0';
	}

	sd->portal = portal;
	sd->target = target;
	sd->port   = port?atoi(port):3260;
	sd->lun    = atoi(lun);
	asprintf(&sd->initiator_name, "%s-%d", options.iscsi_initiatorname, child.id);

	sd->isid  =0x0000800000000000ULL | child.id;
	sd->s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sd->s == -1) {
		printf("could not open socket() errno:%d(%s)\n", errno, strerror(errno));
		return 1;
	}

	sin.sin_family      = AF_INET;
	sin.sin_port        = htons(sd->port);
	if (inet_pton(AF_INET, sd->portal, &sin.sin_addr) != 1) {
		printf("Failed to convert \"%s\" into an address\n", sd->portal);
		return 1;
	}

	if (connect(sd->s, (struct sockaddr *)&sin, sizeof(sin)) != 0) {
		printf("connect failed with errno:%d(%s)\n", errno, strerror(errno));
		return 1;
	}

	sd->itt=0x000a0000;
	sd->cmd_sn=0;
	sd->exp_stat_sn=0;
	if (iscsi_login(sd) != 0) {
		printf("Failed to log in to target.\n");
		return 1;
	}

	fake_op.child=&child;
	fake_op.status="*";
	iscsi_testunitready(&fake_op);

	fake_op.params[0]=0;
	fake_op.params[1]=0;
	fake_op.status="*";
	local_iscsi_readcapacity10(&fake_op, &sd->blocks);

	close(sd->s);
	free(sd);

	return 0;
}

static void iscsi_testunitready(struct dbench_op *op)
{
	struct iscsi_device *sd;
	unsigned char cdb[]={0,0,0,0,0,0};
	int res;
	unsigned char sc;
	int sense_key, sense_ascq;
	unsigned int data_size=0;

	sd = op->child->private;

	res=do_iscsi_io(sd, cdb, sizeof(cdb), SG_DXFER_NONE, &data_size, NULL, &sc, &sense_key, &sense_ascq);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] TESTUNITREADY \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		if (sc == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:%s(%d) Ascq:%s(0x%04x)\n",
		       	     scsi_status_name(sense_key, &scsi_key_names[0]), sense_key, scsi_status_name(sense_ascq, &scsi_ascq_names[0]), sense_ascq);
	        }
		failed(op->child);
	}
	return;
}

static void local_iscsi_readcapacity10(struct dbench_op *op, uint64_t *blocks)
{
	struct iscsi_device *sd;
	unsigned char cdb[]={0x25,0,0,0,0,0,0,0,0,0};
	int res;
	int lba = op->params[0];
	int pmi = op->params[1];
	unsigned int data_size=8;
	char data[data_size];
	unsigned char sc;
	int sense_key, sense_ascq;

	cdb[2] = (lba>>24)&0xff;
	cdb[3] = (lba>>16)&0xff;
	cdb[4] = (lba>> 8)&0xff;
	cdb[5] = (lba    )&0xff;

	cdb[8] = (pmi?1:0);

	sd = op->child->private;

	res=do_iscsi_io(sd, cdb, sizeof(cdb), SG_DXFER_FROM_DEV, &data_size, data, &sc, &sense_key, &sense_ascq);
	if(res){
		printf("SCSI_IO failed\n");
		failed(op->child);
	}
	if (!check_sense(sc, op->status)) {
		printf("[%d] READCAPACITY10 \"%s\" failed (0x%02x) - expected %s\n", 
		       op->child->line, op->fname, sc, op->status);
		if (sc == SCSI_STATUS_CHECK_CONDITION) {
		       printf("SCSI command failed with CHECK_CONDITION. Sense key:%s(%d) Ascq:%s(0x%04x)\n",
		       	     scsi_status_name(sense_key, &scsi_key_names[0]), sense_key, scsi_status_name(sense_ascq, &scsi_ascq_names[0]), sense_ascq);
	        }
		failed(op->child);
	}
	if (blocks) {
		*blocks  = (data[0]&0xff)<<24;
		*blocks |= (data[1]&0xff)<<16;
		*blocks |= (data[2]&0xff)<<8;
		*blocks |= (data[3]&0xff);
	}
}

static struct backend_op ops[] = {
	{ "TESTUNITREADY",      iscsi_testunitready },
	{ "READ10",             iscsi_read10 },
	{ "READCAPACITY10",     iscsi_readcapacity10 },
	{ "SYNCHRONIZECACHE10", iscsi_synchronizecache10 },
	{ "PROUT",              iscsi_prout },
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
