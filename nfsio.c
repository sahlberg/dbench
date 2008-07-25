/* 
   nfs backend for dbench

   Copyright (C) 2008 by Ronnie Sahlberg (ronniesahlberg@gmail.com)
   
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

#include "mount.h"
#include "nfs.h"
#include "libnfs.h"
#include "dbench.h"

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

#define MAX_FILES 200

static char rw_buf[65536];


void nb_sleep(struct child_struct *child, int usec, const char *status)
{
	(void)child;
	(void)usec;
	(void)status;
	usleep(usec);
}

struct cb_data {
	struct nfsio *nfsio;
	char *dirname;
};

static void dirent_cb(struct entryplus3 *e, void *private_data)
{
	struct cb_data *cbd = private_data;
	nfsstat3 res;
	char *objname;

	if (!strcmp(cbd->dirname,".")) {
		return;
	}
	if (!strcmp(cbd->dirname,"..")) {
		return;
	}

	asprintf(&objname, "%s/%s", cbd->dirname, e->name);
	if (objname == NULL) {
		printf("Failed to talloc ne object name in dirent_cb\n");
		exit(10);
	}

	if (e->name_attributes.post_op_attr_u.attributes.type == NF3DIR) {
		struct cb_data *new_cbd = malloc(sizeof(struct cb_data));

		new_cbd->nfsio = cbd->nfsio;
		new_cbd->dirname = strdup(objname);

		nfsio_readdirplus(cbd->nfsio, objname, dirent_cb, new_cbd);
		
		res = nfsio_rmdir(cbd->nfsio, objname);
		if (res != NFS3_OK) {
			printf("Failed to remove object : \"%s\"  %s (%d)\n", objname, nfs_error(res), res);
			free(objname);
			free(new_cbd->dirname);
			free(new_cbd);
			exit(10);
		}


		free(objname);
		free(new_cbd->dirname);
		free(new_cbd);
		return;
	}

	res = nfsio_remove(cbd->nfsio, objname);
	if (res != NFS3_OK) {
		printf("Failed to remove object : \"%s\" %s %d\n", objname, nfs_error(res), res);
		free(objname);
		exit(10);
	}

	free(objname);
}

static void nfs3_deltree(struct child_struct *child, const char *dname)
{
	struct cb_data *cbd;
	nfsstat3 res;
	
	cbd = malloc(sizeof(struct cb_data));

	cbd->nfsio = child->private;
	cbd->dirname = discard_const(dname);

	res = nfsio_lookup(cbd->nfsio, cbd->dirname, NULL);
	if (res != NFS3ERR_NOENT) {
		nfsio_readdirplus(cbd->nfsio, cbd->dirname, dirent_cb, cbd);
		nfsio_rmdir(cbd->nfsio, cbd->dirname);
	}

	res = nfsio_lookup(cbd->nfsio, cbd->dirname, NULL);
	if (res != NFS3ERR_NOENT) {
		printf("Directory \"%s\" not empty. Aborting\n");
		free(cbd);
		exit(10);
	}
	free(cbd);
}

static int expected_status(const char *status)
{
	if (strncmp(status, "0x", 2) == 0) {
		return strtol(status, NULL, 16);
	}
}

static void failed(struct child_struct *child)
{
	child->failed = 1;
	printf("ERROR: child %d failed at line %d\n", child->id, child->line);
	exit(1);
}

static void nfs3_getattr(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_getattr(child->private, fname, NULL);
	if (res != expected_status(status)) {
		printf("[%d] GETATTR \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}


static void nfs3_lookup(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_lookup(child->private, fname, NULL);
	if (res != expected_status(status)) {
		printf("[%d] LOOKUP \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_create(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_create(child->private, fname);
	if (res != expected_status(status)) {
		printf("[%d] CREATE \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_write(struct child_struct *child, const char *fname, int offset, int len, int stable, const char *status)
{
	nfsstat3 res;

	res = nfsio_write(child->private, fname, rw_buf, offset, len, stable);
	if (res != expected_status(status)) {
		printf("[%d] WRITE \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname,
		       res, expected_status(status));
		failed(child);
	}
	child->bytes += len;
}

static void nfs3_commit(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_commit(child->private, fname);
	if (res != expected_status(status)) {
		printf("[%d] COMMIT \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}


static void nfs3_read(struct child_struct *child, const char *fname, int offset, int len, const char *status)
{
	nfsstat3 res = 0;

	res = nfsio_read(child->private, fname, rw_buf, offset, len, NULL, NULL);
	if (res != expected_status(status)) {
		printf("[%d] READ \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname,
		       res, expected_status(status));
		failed(child);
	}
	child->bytes += len;
}

static void nfs3_access(struct child_struct *child, const char *fname, int desired, int granted, const char *status)
{
	nfsstat3 res;

	res = nfsio_access(child->private, fname, 0, NULL);
	if (res != expected_status(status)) {
		printf("[%d] ACCESS \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_mkdir(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_mkdir(child->private, fname);
	if (res != expected_status(status)) {
		printf("[%d] MKDIR \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_rmdir(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_rmdir(child->private, fname);
	if (res != expected_status(status)) {
		printf("[%d] RMDIR \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_fsstat(struct child_struct *child, const char *status)
{
	nfsstat3 res;

	res = nfsio_fsstat(child->private);
	if (res != expected_status(status)) {
		printf("[%d] FSSTAT failed (%x) - expected %x\n", 
		       child->line, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_fsinfo(struct child_struct *child, const char *status)
{
	nfsstat3 res;

	res = nfsio_fsinfo(child->private);
	if (res != expected_status(status)) {
		printf("[%d] FSINFO failed (%x) - expected %x\n", 
		       child->line, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_cleanup(struct child_struct *child)
{
	char *dname;

	asprintf(&dname, "/clients/client%d", child->id);
	nfs3_deltree(child, dname);
	free(dname);
}

static void nfs3_setup(struct child_struct *child)
{
	const char *status = "0x00000000";

	child->rate.last_time = timeval_current();
	child->rate.last_bytes = 0;


	srandom(getpid() ^ time(NULL));
	child->private = nfsio_connect(options.server, options.export, options.protocol);

	if (child->private == NULL) {
		child->failed = 1;
		printf("nfsio_connect() failed\n");
		exit(10);
	}

}

static void nfs3_symlink(struct child_struct *child, const char *fname, const char *fname2, const char *status)
{
	nfsstat3 res;

	res = nfsio_symlink(child->private, fname, fname2);
	if (res != expected_status(status)) {
		printf("[%d] SYMLINK \"%s\"->\"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, fname2,
		       res, expected_status(status));
		failed(child);
	}
}

static void nfs3_remove(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_remove(child->private, fname);
	if (res != expected_status(status)) {
		printf("[%d] REMOVE \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_readdirplus(struct child_struct *child, const char *fname, const char *status)
{
	nfsstat3 res;

	res = nfsio_readdirplus(child->private, fname, NULL, NULL);
	if (res != expected_status(status)) {
		printf("[%d] READDIRPLUS \"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, res, expected_status(status));
		failed(child);
	}
}

static void nfs3_link(struct child_struct *child, const char *fname, const char *fname2, const char *status)
{
	nfsstat3 res;

	res = nfsio_link(child->private, fname, fname2);
	if (res != expected_status(status)) {
		printf("[%d] LINK \"%s\"->\"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, fname2,
		       res, expected_status(status));
		failed(child);
	}
}

static void nfs3_rename(struct child_struct *child, const char *fname, const char *fname2, const char *status)
{
	nfsstat3 res;

	res = nfsio_rename(child->private, fname, fname2);
	if (res != expected_status(status)) {
		printf("[%d] RENAME \"%s\"->\"%s\" failed (%x) - expected %x\n", 
		       child->line, fname, fname2,
		       res, expected_status(status));
		failed(child);
	}
}

struct nb_operations nb_ops = {
	.setup 		= nfs3_setup,
	.deltree	= nfs3_deltree,
	.cleanup	= nfs3_cleanup,

	.getattr3	= nfs3_getattr,
	.lookup3	= nfs3_lookup,
	.create3	= nfs3_create,
	.write3		= nfs3_write,
	.commit3	= nfs3_commit,
	.read3		= nfs3_read,
	.access3	= nfs3_access,
	.mkdir3		= nfs3_mkdir,
	.rmdir3		= nfs3_rmdir,
	.fsstat3	= nfs3_fsstat,
	.fsinfo3	= nfs3_fsinfo,
	.symlink3	= nfs3_symlink,
	.remove3	= nfs3_remove,
	.readdirplus3	= nfs3_readdirplus,
	.rename3	= nfs3_rename,
	.link3		= nfs3_link,
};
