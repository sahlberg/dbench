/* 
   libnfs glue for dbench

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
#define _FILE_OFFSET_BITS 64

#include "config.h"
#ifdef HAVE_LIBNFS

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <poll.h>
#include <errno.h>

#include <inttypes.h>

#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw.h>
#include <nfsc/libnfs-raw-nfs.h>
#include <nfsc/libnfs-raw-nlm.h>
#include "libnfs-glue.h"

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))
#define _U_ __attribute__((unused))

struct nfs_fh3 *nfs_get_rootfh(struct nfs_context *nfs);
void nfs_set_error(struct nfs_context *nfs, char *error_string, ...);
int rpc_nfs_pathconf_async(struct rpc_context *rpc, rpc_cb cb, struct nfs_fh3 *fh, void *private_data);

typedef struct _tree_t {
	nfs_fh3 key;
	nfs_fh3 fh;
	off_t  file_size;
	struct _tree_t *parent;
	struct _tree_t *left;
	struct _tree_t *right;
} tree_t;


struct nfsio {
	struct nfs_context *nfs;
	struct rpc_context *nlm;
	int child;
	unsigned long xid;
	int xid_stride;
	tree_t *fhandles;
};

static void set_xid_value(struct nfsio *nfsio)
{
	nfsio->xid += nfsio->xid_stride;
	rpc_set_next_xid(nfs_get_rpc_context(nfsio->nfs), nfsio->xid);
}

static void free_node(tree_t *t)
{
	free(discard_const(t->key.data.data_val));
	free(discard_const(t->fh.data.data_val));
	free(t);
}

static tree_t *find_fhandle(tree_t *tree, const char *key)
{
	int i;

	if (tree == NULL) {
		return NULL;
	}

	i = strcmp(key, tree->key.data.data_val);
	if (i == 0) {
		return tree;
	}
	if (i < 0) {
		return find_fhandle(tree->left, key);
	}

	return find_fhandle(tree->right, key);
}

static nfs_fh3 *recursive_lookup_fhandle(struct nfsio *nfsio, const char *name)
{
	tree_t *t;
	char *strp;
	char *tmpname;
	nfsstat3 ret;
	
	while (name[0] == '.') name++;

	if (name[0] == 0) {
		return NULL;
	}

	tmpname = strdup(name);
	strp = rindex(tmpname, '/');
	if (strp == NULL) {
		free(tmpname);
		return NULL;
	}
	*strp = 0;

	recursive_lookup_fhandle(nfsio, tmpname);
	free(tmpname);

	t = find_fhandle(nfsio->fhandles, name);
	if (t != NULL) {
		return &t->fh;
	}

	ret = nfsio_lookup(nfsio, name, NULL);
	if (ret != 0) {
		return NULL;
	}

	t = find_fhandle(nfsio->fhandles, name);
	if (t != NULL) {
		return &t->fh;
	}

	return NULL;
}

static nfs_fh3 *lookup_fhandle(struct nfsio *nfsio, const char *name, off_t *off)
{
	tree_t *t;

	while (name[0] == '.') name++;

	if (name[0] == 0) {
		name = "/";
	}

	t = find_fhandle(nfsio->fhandles, name);
	if (t == NULL) {
		return recursive_lookup_fhandle(nfsio, name);
	}

	if (off) {
		*off = t->file_size;
	}

	return &t->fh;
}

static void delete_fhandle(struct nfsio *nfsio, const char *name)
{
	tree_t *t;

	while (name[0] == '.') name++;

	t = find_fhandle(nfsio->fhandles, name);
	if (t == NULL) {
		return;
	}

	/* we have a left child */
	if (t->left) {
		tree_t *tmp_tree;

		for(tmp_tree=t->left;tmp_tree->right;tmp_tree=tmp_tree->right)
			;
		tmp_tree->right = t->right;
		if (t->right) {
			t->right->parent = tmp_tree;
		}

		if (t->parent == NULL) {
			nfsio->fhandles = tmp_tree;
			tmp_tree->parent = NULL;
			free_node(t);
			return;
		}

		if (t->parent->left == t) {
			t->parent->left = t->left;
			if (t->left) {
				t->left->parent = t->parent;
			}
			free_node(t);
			return;
		}

		t->parent->right = t->left;
		if (t->left) {
			t->left->parent = t->parent;
		}
		free_node(t);
		return;
	}

	/* we only have a right child */
	if (t->right) {
		tree_t *tmp_tree;

		for(tmp_tree=t->right;tmp_tree->left;tmp_tree=tmp_tree->left)
			;
		tmp_tree->left = t->left;
		if (t->left) {
			t->left->parent = tmp_tree;
		}

		if (t->parent == NULL) {
			nfsio->fhandles = tmp_tree;
			tmp_tree->parent = NULL;
			free_node(t);
			return;
		}

		if (t->parent->left == t) {
			t->parent->left = t->right;
			if (t->right) {
				t->right->parent = t->parent;
			}
			free_node(t);
			return;
		}

		t->parent->right = t->right;
		if (t->right) {
			t->right->parent = t->parent;
		}
		free_node(t);
		return;
	}

	/* we are a leaf node */
	if (t->parent == NULL) {
		nfsio->fhandles = NULL;
	} else {
		if (t->parent->left == t) {
			t->parent->left = NULL;
		} else {
			t->parent->right = NULL;
		}
	}
	free_node(t);
	return;
}

static void insert_fhandle(struct nfsio *nfsio, const char *name, const char *fhandle, int length, off_t off)
{
	tree_t *tmp_t;
	tree_t *t;
	int i;

	while (name[0] == '.') name++;

	t = malloc(sizeof(tree_t));
	if (t == NULL) {
		fprintf(stderr, "MALLOC failed to allocate tree_t in insert_fhandle\n");
		exit(10);
	}

	t->key.data.data_val = strdup(name);
	if (t->key.data.data_val == NULL) {
		fprintf(stderr, "STRDUP failed to allocate key in insert_fhandle\n");
		exit(10);
	}
	t->key.data.data_len = strlen(name);


	t->fh.data.data_val = malloc(length);
	if (t->key.data.data_val == NULL) {
		fprintf(stderr, "MALLOC failed to allocate fhandle in insert_fhandle\n");
		exit(10);
	}
	memcpy(discard_const(t->fh.data.data_val), fhandle, length);
	t->fh.data.data_len = length;	
	
	t->file_size = off;
	t->left   = NULL;
	t->right  = NULL;
	t->parent = NULL;

	if (nfsio->fhandles == NULL) {
		nfsio->fhandles = t;
		return;
	}

	tmp_t = nfsio->fhandles;
again:
	i = strcmp(t->key.data.data_val, tmp_t->key.data.data_val);
	if (i == 0) {
		free(discard_const(tmp_t->fh.data.data_val));
		tmp_t->fh.data.data_len = t->fh.data.data_len;
		tmp_t->fh.data.data_val  = t->fh.data.data_val;
		free(discard_const(t->key.data.data_val));
		free(t);
		return;
	}
	if (i < 0) {
		if (tmp_t->left == NULL) {
			tmp_t->left = t;
			t->parent = tmp_t;
			return;
		}
		tmp_t = tmp_t->left;
		goto again;
	}
	if (tmp_t->right == NULL) {
		tmp_t->right = t;
		t->parent = tmp_t;
		return;
	}
	tmp_t = tmp_t->right;
	goto again;
}


struct nfs_errors {
	const char *err;
	int idx;
};

static const struct nfs_errors nfs_errors[] = {
	{"NFS3_OK", 0},
	{"NFS3ERR_PERM", 1},
	{"NFS3ERR_NOENT", 2},
	{"NFS3ERR_IO", 5},
	{"NFS3ERR_NXIO", 6},
	{"NFS3ERR_ACCES", 13},
	{"NFS3ERR_EXIST", 17},
	{"NFS3ERR_XDEV", 18},
	{"NFS3ERR_NODEV", 19},
	{"NFS3ERR_NOTDIR", 20},
	{"NFS3ERR_ISDIR", 21},
	{"NFS3ERR_INVAL", 22},
	{"NFS3ERR_FBIG", 27},
	{"NFS3ERR_NOSPC", 28},
	{"NFS3ERR_ROFS", 30},
	{"NFS3ERR_MLINK", 31},
	{"NFS3ERR_NAMETOOLONG", 63},
	{"NFS3ERR_NOTEMPTY", 66},
	{"NFS3ERR_DQUOT", 69},
	{"NFS3ERR_STALE", 70},
	{"NFS3ERR_REMOTE", 71},
	{"NFS3ERR_BADHANDLE", 10001},
	{"NFS3ERR_NOT_SYNC", 10002},
	{"NFS3ERR_BAD_COOKIE", 10003},
	{"NFS3ERR_NOTSUPP", 10004},
	{"NFS3ERR_TOOSMALL", 10005},
	{"NFS3ERR_SERVERFAULT", 10006},
	{"NFS3ERR_BADTYPE", 10007},
	{"NFS3ERR_JUKEBOX", 10008},
};



const char *nfs_error(int error)
{
	unsigned int i;

	for(i=0;i<sizeof(nfs_errors)/sizeof(struct nfs_errors);i++) {
		if (error == nfs_errors[i].idx) {
			return nfs_errors[i].err;
		}
	}
	return "Unknown NFS error";
}

struct nfsio_cb_data {
	struct nfsio *nfsio;
	char *name, *old_name;
	fattr3 *attributes;
	uint32_t *access;
	nfs_fh3 *fh;
	nfs3_dirent_cb rd_cb;
	void *private_data;

	int is_finished;
	int status;
};

static void nfsio_wait_for_rpc_reply(struct rpc_context *rpc, struct nfsio_cb_data *cb_data)
{
	struct pollfd pfd;

	while (!cb_data->is_finished) {

		pfd.fd = rpc_get_fd(rpc);
		pfd.events = rpc_which_events(rpc);
		if (poll(&pfd, 1, -1) < 0) {
			rpc_set_error(rpc, "Poll failed");
			cb_data->status = -EIO;
			break;
		}
		if (rpc_service(rpc, pfd.revents) < 0) {
			rpc_set_error(rpc, "nfs_service failed");
			cb_data->status = -EIO;
			break;
		}
	}
}

static void nfsio_wait_for_nfs_reply(struct nfs_context *nfs, struct nfsio_cb_data *cb_data)
{
	return nfsio_wait_for_rpc_reply(nfs_get_rpc_context(nfs), cb_data);
}

void nfsio_disconnect(struct nfsio *nfsio)
{
	if (nfsio->nfs != NULL) {
		nfs_destroy_context(nfsio->nfs);
		nfsio->nfs = NULL;
	}
	if (nfsio->nlm != NULL) {
		rpc_destroy_context(nfsio->nlm);
		nfsio->nlm = NULL;
	}

	free(nfsio);
}


void nlm_connect_cb(struct rpc_context *rpc, int status, void *data, void *private_data)
{
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;
}

struct nfsio *nfsio_connect(const char *url, int child, int initial_xid, int xid_stride, int nlm)
{
	struct nfsio *nfsio;
	char *tmp, *server, *export;
	char *child_name;
	struct nfs_fh3 *root_fh;

	tmp = strdup(url);
	if (tmp == NULL) {
		fprintf(stderr, "Failed to strdup nfs url\n");
		return NULL;
	}
	if (strncmp(tmp, "nfs://", 6)) {
		fprintf(stderr, "Invalid URL. NFS URL must be of form nfs://<server>/<path>\n");
		free(tmp);
		return NULL;
	}
	server = &tmp[6];
	export = strchr(server, '/');
	if (export == NULL) {
		fprintf(stderr, "Invalid URL. NFS URL must be of form nfs://<server>/<path>\n");
		free(tmp);
		return NULL;
	}
	*export = 0;

	export = strchr(&url[6], '/');

	nfsio = malloc(sizeof(struct nfsio));
	if (nfsio == NULL) {
		fprintf(stderr, "Failed to malloc nfsio\n");
		free(tmp);
		return NULL;
	}
	memset(nfsio, 0, sizeof(struct nfsio));

	nfsio->xid        = initial_xid;
	nfsio->xid_stride = xid_stride;
	nfsio->nfs = nfs_init_context();

	if (nfs_mount(nfsio->nfs, server, export) != 0) {
		fprintf(stderr, "Failed to mount %s. Error:%s\n", url,
			nfs_get_error(nfsio->nfs));
		free(tmp);
		return NULL;
	}

	nfsio->child = child;
	asprintf(&child_name, "dbench-child-%d", nfsio->child);
	nfs_set_auth(nfsio->nfs, libnfs_authunix_create(child_name, getuid(), getpid(), 0, NULL));
	free(child_name);

	root_fh = nfs_get_rootfh(nfsio->nfs);
	insert_fhandle(nfsio, "/",
			      root_fh->data.data_val,
			      root_fh->data.data_len,
			      0);
	if (nlm) {
		struct nfsio_cb_data cb_data;

		memset(&cb_data, 0, sizeof(cb_data));
		cb_data.nfsio = nfsio;

		nfsio->nlm = rpc_init_context();
		if (nfsio->nlm == NULL) {
			printf("failed to init nlm context\n");
			free(tmp);
			exit(10);
		}
		if (rpc_connect_program_async(nfsio->nlm, server, 100021, 4,
					      nlm_connect_cb, &cb_data) != 0) {
			printf("Failed to start NLM connection. %s\n",
				rpc_get_error(nfsio->nlm));
			free(tmp);
			exit(10);
		}
		nfsio_wait_for_rpc_reply(nfsio->nlm, &cb_data);
	}
	free(tmp);

	return nfsio;
}

static void nfsio_getattr_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct GETATTR3res *GETATTR3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (GETATTR3res->status != NFS3_OK) {
		cb_data->status = GETATTR3res->status;
		return;
	}

	if (cb_data->attributes) {
		memcpy(cb_data->attributes,
			&GETATTR3res->GETATTR3res_u.resok.obj_attributes,
			sizeof(fattr3));
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_getattr(struct nfsio *nfsio, const char *name, fattr3 *attributes)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_getattr\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.attributes = attributes;

	set_xid_value(nfsio);
	if (rpc_nfs_getattr_async(nfs_get_rpc_context(nfsio->nfs),
		nfsio_getattr_cb, fh, &cb_data)) {
		fprintf(stderr, "failed to send getattr\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_lookup_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct LOOKUP3res *LOOKUP3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (LOOKUP3res->status != NFS3_OK) {
		cb_data->status = LOOKUP3res->status;
		return;
	}

	insert_fhandle(cb_data->nfsio, cb_data->name, 
			LOOKUP3res->LOOKUP3res_u.resok.object.data.data_val,
			LOOKUP3res->LOOKUP3res_u.resok.object.data.data_len,
			LOOKUP3res->LOOKUP3res_u.resok.obj_attributes.post_op_attr_u.attributes.size);

	if (cb_data->attributes) {
		memcpy(cb_data->attributes,
			&LOOKUP3res->LOOKUP3res_u.resok.obj_attributes.post_op_attr_u.attributes,
			sizeof(fattr3));
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_lookup(struct nfsio *nfsio, const char *name, fattr3 *attributes)
{
	char *tmp_name;
	char *ptr;
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_lookup\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_lookup\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle for '%s' in nfsio_lookup\n", tmp_name);
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name = discard_const(name);
	cb_data.attributes = attributes;

	set_xid_value(nfsio);
	if (rpc_nfs_lookup_async(nfs_get_rpc_context(nfsio->nfs),
		nfsio_lookup_cb, fh, ptr, &cb_data)) {
		fprintf(stderr, "failed to send lookup for '%s' "
			"in nfsio_lookup\n", tmp_name);
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_access_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct ACCESS3res *ACCESS3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (ACCESS3res->status != NFS3_OK) {
		cb_data->status = ACCESS3res->status;
		return;
	}

	if (cb_data->access) {
		*cb_data->access = ACCESS3res->ACCESS3res_u.resok.access;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_access(struct nfsio *nfsio, const char *name, uint32_t desired, uint32_t *access)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_access\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.access = access;

	set_xid_value(nfsio);
	if (rpc_nfs_access_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_access_cb, fh, desired, &cb_data)) {
		fprintf(stderr, "failed to send access\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}


static void nfsio_create_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct CREATE3res *CREATE3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (CREATE3res->status != NFS3_OK) {
		cb_data->status = CREATE3res->status;
		return;
	}

	insert_fhandle(cb_data->nfsio, cb_data->name, 
			CREATE3res->CREATE3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
			CREATE3res->CREATE3res_u.resok.obj.post_op_fh3_u.handle.data.data_len,
			CREATE3res->CREATE3res_u.resok.obj_attributes.post_op_attr_u.attributes.size);

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_create(struct nfsio *nfsio, const char *name)
{
	struct CREATE3args CREATE3args;
	char *tmp_name, *ptr;
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_create\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_create\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_create\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&CREATE3args, 0, sizeof(CREATE3args));
	CREATE3args.where.dir  = *fh;
	CREATE3args.where.name = ptr;

	CREATE3args.how.mode = UNCHECKED;
	CREATE3args.how.createhow3_u.obj_attributes.mode.set_it  = TRUE;
	CREATE3args.how.createhow3_u.obj_attributes.mode.set_mode3_u.mode    = 0666;
	CREATE3args.how.createhow3_u.obj_attributes.uid.set_it   = TRUE;
	CREATE3args.how.createhow3_u.obj_attributes.uid.set_uid3_u.uid      = 0;
	CREATE3args.how.createhow3_u.obj_attributes.gid.set_it   = TRUE;
	CREATE3args.how.createhow3_u.obj_attributes.gid.set_gid3_u.gid      = 0;
	CREATE3args.how.createhow3_u.obj_attributes.size.set_it  = FALSE;
	CREATE3args.how.createhow3_u.obj_attributes.atime.set_it = FALSE;
	CREATE3args.how.createhow3_u.obj_attributes.mtime.set_it = FALSE;

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name = discard_const(name);

	set_xid_value(nfsio);
	if (rpc_nfs_create_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_create_cb, &CREATE3args, &cb_data)) {
		fprintf(stderr, "failed to send create\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_remove_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct REMOVE3res *REMOVE3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (REMOVE3res->status != NFS3_OK) {
		cb_data->status = REMOVE3res->status;
		return;
	}

	delete_fhandle(cb_data->nfsio, cb_data->name);

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_remove(struct nfsio *nfsio, const char *name)
{
	char *tmp_name, *ptr;
	nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_remove\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_remove\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_remove\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name = discard_const(name);

	set_xid_value(nfsio);
	if (rpc_nfs_remove_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_remove_cb, fh, ptr, &cb_data)) {
		fprintf(stderr, "failed to send remove\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_write_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct WRITE3res *WRITE3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (WRITE3res->status != NFS3_OK) {
		cb_data->status = WRITE3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_write(struct nfsio *nfsio, const char *name, char *buf, uint64_t offset, int len, int stable)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_write\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_write_async(nfs_get_rpc_context(nfsio->nfs), nfsio_write_cb,
				fh, buf, offset, len, stable, &cb_data)) {
		fprintf(stderr, "failed to send write\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_read_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct READ3res *READ3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (READ3res->status != NFS3_OK) {
		cb_data->status = READ3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_read(struct nfsio *nfsio, const char *name, char *buf _U_, uint64_t offset, int len)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_read\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_read_async(nfs_get_rpc_context(nfsio->nfs), nfsio_read_cb,
			fh, offset, len, &cb_data)) {
		fprintf(stderr, "failed to send read\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

void nfsio_lock_cb(struct rpc_context *rpc, int status, void *data, void *private_data)
{
	struct NLM4_LOCKres *NLM4_LOCKres = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (NLM4_LOCKres->status != NLM4_GRANTED) {
		cb_data->status = NLM4_LOCKres->status;
		return;
	}

	cb_data->status = NLM4_GRANTED;
}

nlmstat4 nfsio_lock(struct nfsio *nfsio, const char *name, uint64_t offset, int len)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;
	struct NLM4_LOCKargs NLM4_LOCKargs;
	uint32_t cookie = time(NULL) ^ getpid();

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_lock\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	memset(&NLM4_LOCKargs, 0, sizeof(NLM4_LOCKargs));
	NLM4_LOCKargs.cookie.data.data_len  = sizeof(cookie);
	NLM4_LOCKargs.cookie.data.data_val  = (char *)&cookie;
	NLM4_LOCKargs.block                 = 0;
	NLM4_LOCKargs.exclusive             = 1;
	NLM4_LOCKargs.lock.caller_name      = "dbench";
	NLM4_LOCKargs.lock.fh.data.data_len = fh->data.data_len;
	NLM4_LOCKargs.lock.fh.data.data_val = fh->data.data_val;
	NLM4_LOCKargs.lock.oh               = "dbench";
	NLM4_LOCKargs.lock.svid             = nfsio->child;
	NLM4_LOCKargs.lock.l_offset = offset;
	NLM4_LOCKargs.lock.l_len    = len;
	NLM4_LOCKargs.reclaim = 0;
	NLM4_LOCKargs.state = 0;

	if (rpc_nlm4_lock_async(nfsio->nlm, nfsio_lock_cb,
			&NLM4_LOCKargs, &cb_data)) {
		fprintf(stderr, "failed to send lock\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_rpc_reply(nfsio->nlm, &cb_data);

	return cb_data.status;
}

void nfsio_unlock_cb(struct rpc_context *rpc, int status, void *data, void *private_data)
{
	struct NLM4_UNLOCKres *NLM4_UNLOCKres = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (NLM4_UNLOCKres->status != NLM4_GRANTED) {
		cb_data->status = NLM4_UNLOCKres->status;
		return;
	}

	cb_data->status = NLM4_GRANTED;
}

nlmstat4 nfsio_unlock(struct nfsio *nfsio, const char *name, uint64_t offset, int len)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;
	struct NLM4_UNLOCKargs NLM4_UNLOCKargs;
	uint32_t cookie = time(NULL) ^ getpid();

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_unlock\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	memset(&NLM4_UNLOCKargs, 0, sizeof(NLM4_UNLOCKargs));
	NLM4_UNLOCKargs.cookie.data.data_len  = sizeof(cookie);
	NLM4_UNLOCKargs.cookie.data.data_val  = (char *)&cookie;
	NLM4_UNLOCKargs.lock.caller_name      = "dbench";
	NLM4_UNLOCKargs.lock.fh.data.data_len = fh->data.data_len;
	NLM4_UNLOCKargs.lock.fh.data.data_val = fh->data.data_val;
	NLM4_UNLOCKargs.lock.oh               = "dbench";
	NLM4_UNLOCKargs.lock.svid             = nfsio->child;
	NLM4_UNLOCKargs.lock.l_offset = offset;
	NLM4_UNLOCKargs.lock.l_len    = len;

	if (rpc_nlm4_unlock_async(nfsio->nlm, nfsio_unlock_cb,
			&NLM4_UNLOCKargs, &cb_data)) {
		fprintf(stderr, "failed to send unlock\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_rpc_reply(nfsio->nlm, &cb_data);

	return cb_data.status;
}

void nfsio_test_cb(struct rpc_context *rpc, int status, void *data, void *private_data)
{
	struct NLM4_TESTres *NLM4_TESTres = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (NLM4_TESTres->reply.status != NLM4_GRANTED) {
		cb_data->status = NLM4_TESTres->reply.status;
		return;
	}

	cb_data->status = NLM4_GRANTED;
}

nlmstat4 nfsio_test(struct nfsio *nfsio, const char *name, uint64_t offset, int len)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;
	struct NLM4_TESTargs NLM4_TESTargs;
	uint32_t cookie = time(NULL) ^ getpid();

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_test\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	memset(&NLM4_TESTargs, 0, sizeof(NLM4_TESTargs));
	NLM4_TESTargs.cookie.data.data_len  = sizeof(cookie);
	NLM4_TESTargs.cookie.data.data_val  = (char *)&cookie;
	NLM4_TESTargs.exclusive             = 1;
	NLM4_TESTargs.lock.caller_name      = "dbench";
	NLM4_TESTargs.lock.fh.data.data_len = fh->data.data_len;
	NLM4_TESTargs.lock.fh.data.data_val = fh->data.data_val;
	NLM4_TESTargs.lock.oh               = "dbench";
	NLM4_TESTargs.lock.svid             = nfsio->child;
	NLM4_TESTargs.lock.l_offset = offset;
	NLM4_TESTargs.lock.l_len    = len;

	if (rpc_nlm4_test_async(nfsio->nlm, nfsio_test_cb,
			&NLM4_TESTargs, &cb_data)) {
		fprintf(stderr, "failed to send test\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_rpc_reply(nfsio->nlm, &cb_data);

	return cb_data.status;
}
static void nfsio_commit_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct COMMIT3res *COMMIT3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (COMMIT3res->status != NFS3_OK) {
		cb_data->status = COMMIT3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_commit(struct nfsio *nfsio, const char *name)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_commit\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_commit_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_commit_cb, fh, &cb_data)) {
		fprintf(stderr, "failed to send commit\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_fsinfo_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct FSINFO3res *FSINFO3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (FSINFO3res->status != NFS3_OK) {
		cb_data->status = FSINFO3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_fsinfo(struct nfsio *nfsio)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, "/", NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_fsinfo\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_fsinfo_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_fsinfo_cb, fh, &cb_data)) {
		fprintf(stderr, "failed to send fsinfo\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}


static void nfsio_fsstat_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct FSSTAT3res *FSSTAT3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (FSSTAT3res->status != NFS3_OK) {
		cb_data->status = FSSTAT3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_fsstat(struct nfsio *nfsio)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, "/", NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_fsstat\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_fsstat_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_fsstat_cb, fh, &cb_data)) {
		fprintf(stderr, "failed to send fsstat\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_pathconf_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct PATHCONF3res *PATHCONF3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (PATHCONF3res->status != NFS3_OK) {
		cb_data->status = PATHCONF3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_pathconf(struct nfsio *nfsio, char *name)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_pathconf\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_pathconf_async(nfs_get_rpc_context(nfsio->nfs),
		nfsio_pathconf_cb, fh, &cb_data)) {
		fprintf(stderr, "failed to send pathconf\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_symlink_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct SYMLINK3res *SYMLINK3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (SYMLINK3res->status != NFS3_OK) {
		cb_data->status = SYMLINK3res->status;
		return;
	}

	insert_fhandle(cb_data->nfsio, cb_data->name, 
		       SYMLINK3res->SYMLINK3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
		       SYMLINK3res->SYMLINK3res_u.resok.obj.post_op_fh3_u.handle.data.data_len,
		       0);

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_symlink(struct nfsio *nfsio, const char *old, const char *new)
{
	char *tmp_name, *ptr;
	nfs_fh3 *fh;
	struct SYMLINK3args SYMLINK3args;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(old);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_symlink\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_symlink\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_symlink\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&SYMLINK3args, 0, sizeof(SYMLINK3args));
	SYMLINK3args.where.dir  = *fh;
	SYMLINK3args.where.name	= ptr;

	SYMLINK3args.symlink.symlink_attributes.mode.set_it = TRUE;
	SYMLINK3args.symlink.symlink_attributes.mode.set_mode3_u.mode = 0777;
	SYMLINK3args.symlink.symlink_attributes.uid.set_it = TRUE;
	SYMLINK3args.symlink.symlink_attributes.uid.set_uid3_u.uid= 0;
	SYMLINK3args.symlink.symlink_attributes.gid.set_it = TRUE;
	SYMLINK3args.symlink.symlink_attributes.gid.set_gid3_u.gid = 0;
	SYMLINK3args.symlink.symlink_attributes.size.set_it = FALSE;
	SYMLINK3args.symlink.symlink_attributes.atime.set_it = FALSE;
	SYMLINK3args.symlink.symlink_attributes.mtime.set_it = FALSE;
	SYMLINK3args.symlink.symlink_data     = discard_const(new);

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name  = discard_const(old);

	set_xid_value(nfsio);
	if (rpc_nfs_symlink_async(nfs_get_rpc_context(nfsio->nfs),
		nfsio_symlink_cb, &SYMLINK3args, &cb_data)) {
		fprintf(stderr, "failed to send symlink\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_link_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct LINK3res *LINK3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (LINK3res->status != NFS3_OK) {
		cb_data->status = LINK3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_link(struct nfsio *nfsio, const char *old, const char *new)
{
	char *tmp_name, *ptr;
	nfs_fh3 *fh, *new_fh;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(old);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_link\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_link\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_link\n");
		return NFS3ERR_SERVERFAULT;
	}

	new_fh = lookup_fhandle(nfsio, new, NULL);
	if (new_fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_link\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name  = ptr;

	set_xid_value(nfsio);
	if (rpc_nfs_link_async(nfs_get_rpc_context(nfsio->nfs),
			       nfsio_link_cb, new_fh, fh, ptr, &cb_data)) {
		fprintf(stderr, "failed to send link\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_readlink_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct READLINK3res *READLINK3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (READLINK3res->status != NFS3_OK) {
		cb_data->status = READLINK3res->status;
		return;
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_readlink(struct nfsio *nfsio, char *name)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;
	READLINK3args READLINK3args;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_readlink\n");
		return NFS3ERR_SERVERFAULT;
	}

	READLINK3args.symlink = *fh;

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;

	set_xid_value(nfsio);
	if (rpc_nfs_readlink_async(nfs_get_rpc_context(nfsio->nfs),
		nfsio_readlink_cb, &READLINK3args, &cb_data)) {
		fprintf(stderr, "failed to send readlink\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_rmdir_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct RMDIR3res *RMDIR3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (RMDIR3res->status != NFS3_OK) {
		cb_data->status = RMDIR3res->status;
		return;
	}

	delete_fhandle(cb_data->nfsio, cb_data->name);

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_rmdir(struct nfsio *nfsio, const char *name)
{
	char *tmp_name, *ptr;
	nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_rmdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_rmdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rmdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name = discard_const(name);

	set_xid_value(nfsio);
	if (rpc_nfs_rmdir_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_rmdir_cb, fh, ptr, &cb_data)) {
		fprintf(stderr, "failed to send rmdir\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_mkdir_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct MKDIR3res *MKDIR3res = data;
	struct nfsio_cb_data *cb_data = private_data;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (MKDIR3res->status != NFS3_OK) {
		cb_data->status = MKDIR3res->status;
		return;
	}

	insert_fhandle(cb_data->nfsio, cb_data->name, 
			MKDIR3res->MKDIR3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
			MKDIR3res->MKDIR3res_u.resok.obj.post_op_fh3_u.handle.data.data_len,
			0);

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_mkdir(struct nfsio *nfsio, const char *name)
{
	struct MKDIR3args MKDIR3args;
	char *tmp_name, *ptr;
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;

	tmp_name = strdupa(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_mkdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_mkdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_mkdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&MKDIR3args, 0, sizeof(MKDIR3args));
	MKDIR3args.where.dir  = *fh;
	MKDIR3args.where.name = ptr;

	MKDIR3args.attributes.mode.set_it  = TRUE;
	MKDIR3args.attributes.mode.set_mode3_u.mode    = 0777;
	MKDIR3args.attributes.uid.set_it   = TRUE;
	MKDIR3args.attributes.uid.set_uid3_u.uid      = 0;
	MKDIR3args.attributes.gid.set_it   = TRUE;
	MKDIR3args.attributes.gid.set_gid3_u.gid      = 0;
	MKDIR3args.attributes.size.set_it  = FALSE;
	MKDIR3args.attributes.atime.set_it = FALSE;
	MKDIR3args.attributes.mtime.set_it = FALSE;

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.name = discard_const(name);

	set_xid_value(nfsio);
	if (rpc_nfs_mkdir_async(nfs_get_rpc_context(nfsio->nfs),
				 nfsio_mkdir_cb, &MKDIR3args, &cb_data)) {
		fprintf(stderr, "failed to send mkdir\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

static void nfsio_readdirplus_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct READDIRPLUS3res *READDIRPLUS3res = data;
	struct nfsio_cb_data *cb_data = private_data;
	entryplus3 *e;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (READDIRPLUS3res->status != NFS3_OK) {
		cb_data->status = READDIRPLUS3res->status;
		return;
	}

	if (READDIRPLUS3res->READDIRPLUS3res_u.resok.reply.eof == 0) {
		for(e = READDIRPLUS3res->READDIRPLUS3res_u.resok.reply.entries;
			e->nextentry; 
			e = e->nextentry){
		}

		set_xid_value(cb_data->nfsio);
		if (rpc_nfs_readdirplus_async(
				nfs_get_rpc_context(cb_data->nfsio->nfs),
				nfsio_readdirplus_cb,
				cb_data->fh,
				e->cookie,
				(char *)&READDIRPLUS3res->READDIRPLUS3res_u.resok.cookieverf,
				8000, cb_data)) {
			fprintf(stderr, "failed to send readdirplus\n");
			cb_data->status = NFS3ERR_SERVERFAULT;
			return;
		}
		cb_data->is_finished = 0;
		nfsio_wait_for_nfs_reply(cb_data->nfsio->nfs, cb_data);
	}

	/* Record the dir/file name to filehandle mappings */
	for(e = READDIRPLUS3res->READDIRPLUS3res_u.resok.reply.entries;
		e; e = e->nextentry){
		char *new_name;

		if(!strcmp(e->name, ".")){
			continue;
		}
		if(!strcmp(e->name, "..")){
			continue;
		}
		if(e->name_handle.handle_follows == 0){
			continue;
		}

		asprintf(&new_name, "%s/%s", cb_data->name, e->name);
		insert_fhandle(cb_data->nfsio, new_name, 
			e->name_handle.post_op_fh3_u.handle.data.data_val,
			e->name_handle.post_op_fh3_u.handle.data.data_len,
			0 /*qqq*/
		);
		free(new_name);

		if (cb_data->rd_cb) {
			cb_data->rd_cb(e, cb_data->private_data);
		}
	}

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_readdirplus(struct nfsio *nfsio, const char *name, nfs3_dirent_cb cb, void *private_data)
{
	struct nfs_fh3 *fh;
	struct nfsio_cb_data cb_data;
	cookieverf3 cv;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle for '%s' in nfsio_readdirplus\n", name);
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cv, 0, sizeof(cv));
	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio = nfsio;
	cb_data.fh    = fh;
	cb_data.name  = name;
	cb_data.rd_cb = cb;
	cb_data.private_data = private_data;

	set_xid_value(nfsio);
	if (rpc_nfs_readdirplus_async(nfs_get_rpc_context(nfsio->nfs),
		nfsio_readdirplus_cb, fh, 0, (char *)&cv, 8000, &cb_data)) {
		fprintf(stderr, "failed to send readdirplus\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}


static void nfsio_rename_cb(struct rpc_context *rpc _U_, int status,
       void *data, void *private_data) {
	struct RENAME3res *RENAME3res = data;
	struct nfsio_cb_data *cb_data = private_data;
	nfs_fh3 *old_fh;

	cb_data->is_finished = 1;

	if (status != RPC_STATUS_SUCCESS) {
		cb_data->status = NFS3ERR_SERVERFAULT;
		return;
	}
	if (RENAME3res->status != NFS3_OK) {
		cb_data->status = RENAME3res->status;
		return;
	}

	old_fh = lookup_fhandle(cb_data->nfsio, cb_data->old_name, NULL);
	delete_fhandle(cb_data->nfsio, cb_data->old_name);
	insert_fhandle(cb_data->nfsio, cb_data->name, 
			old_fh->data.data_val,
			old_fh->data.data_len,
			0 /* FIXME */
		);

	cb_data->status = NFS3_OK;
}

nfsstat3 nfsio_rename(struct nfsio *nfsio, const char *old, const char *new)
{
	char *tmp_old_name;
	char *tmp_new_name;
	nfs_fh3 *old_fh, *new_fh;
	char *old_ptr, *new_ptr;
	struct nfsio_cb_data cb_data;

	tmp_old_name = strdupa(old);
	if (tmp_old_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_rename\n");
		return NFS3ERR_SERVERFAULT;
	}

	old_ptr = rindex(tmp_old_name, '/');
	if (old_ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_rename\n");
		return NFS3ERR_SERVERFAULT;
	}

	*old_ptr = 0;
	old_ptr++;

	old_fh = lookup_fhandle(nfsio, tmp_old_name, NULL);
	if (old_fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rename\n");
		return NFS3ERR_SERVERFAULT;
	}

	tmp_new_name = strdupa(new);
	if (tmp_new_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_rename\n");
		return NFS3ERR_SERVERFAULT;
	}

	new_ptr = rindex(tmp_new_name, '/');
	if (new_ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_rename\n");
		return NFS3ERR_SERVERFAULT;
	}

	*new_ptr = 0;
	new_ptr++;

	new_fh = lookup_fhandle(nfsio, tmp_new_name, NULL);
	if (new_fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rename\n");
		return NFS3ERR_SERVERFAULT;
	}

	memset(&cb_data, 0, sizeof(cb_data));
	cb_data.nfsio    = nfsio;
	cb_data.name     = discard_const(new);
	cb_data.old_name = discard_const(old);

	set_xid_value(nfsio);
	if (rpc_nfs_rename_async(nfs_get_rpc_context(nfsio->nfs),
			nfsio_rename_cb,
			old_fh, old_ptr,
			new_fh, new_ptr,
			&cb_data)) {
		fprintf(stderr, "failed to send rename\n");
		return NFS3ERR_SERVERFAULT;
	}
	nfsio_wait_for_nfs_reply(nfsio->nfs, &cb_data);

	return cb_data.status;
}

#endif /* HAVE_LIBNFS */
