/* 
   nfs library for dbench

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
#include "mount.h"
#include "nfs.h"
#include "libnfs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

typedef struct _data_t {
	const char *dptr;
	int dsize;
} data_t;

typedef struct _tree_t {
	data_t key;
	data_t fh;
	off_t  file_size;
	struct _tree_t *parent;
	struct _tree_t *left;
	struct _tree_t *right;
} tree_t;


struct nfsio {
	int s;
	CLIENT *clnt;
	unsigned long xid;
	int xid_stride;
	tree_t *fhandles;
};

static void set_new_xid(struct nfsio *nfsio)
{
	unsigned long xid = nfsio->xid;

	clnt_control(nfsio->clnt, CLSET_XID, (char *)&xid);
	nfsio->xid += nfsio->xid_stride;
}

static void free_node(tree_t *t)
{
	free(discard_const(t->key.dptr));
	free(discard_const(t->fh.dptr));
	free(t);
}

static tree_t *find_fhandle(tree_t *tree, const char *key)
{
	int i;

	if (tree == NULL) {
		return NULL;
	}

	i = strcmp(key, tree->key.dptr);
	if (i == 0) {
		return tree;
	}
	if (i < 0) {
		return find_fhandle(tree->left, key);
	}

	return find_fhandle(tree->right, key);
}

static data_t *recursive_lookup_fhandle(struct nfsio *nfsio, const char *name)
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

	return ;
}

static data_t *lookup_fhandle(struct nfsio *nfsio, const char *name, off_t *off)
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

	t->key.dptr = strdup(name);
	if (t->key.dptr == NULL) {
		fprintf(stderr, "STRDUP failed to allocate key in insert_fhandle\n");
		exit(10);
	}
	t->key.dsize = strlen(name);


	t->fh.dptr = malloc(length);
	if (t->key.dptr == NULL) {
		fprintf(stderr, "MALLOC failed to allocate fhandle in insert_fhandle\n");
		exit(10);
	}
	memcpy(discard_const(t->fh.dptr), fhandle, length);
	t->fh.dsize = length;	
	
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
	i = strcmp(t->key.dptr, tmp_t->key.dptr);
	if (i == 0) {
		free(discard_const(tmp_t->fh.dptr));
		tmp_t->fh.dsize = t->fh.dsize;
		tmp_t->fh.dptr  = t->fh.dptr;
		free(discard_const(t->key.dptr));
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




void nfsio_disconnect(struct nfsio *nfsio)
{
	if (nfsio->clnt != NULL) {
		clnt_destroy(nfsio->clnt);
		nfsio->clnt = NULL;
	}
	if (nfsio->s != -1) {
		close(nfsio->s);
	}
	// qqq free the tree*/

	free(nfsio);
}




struct nfsio *nfsio_connect(const char *server, const char *export, const char *protocol, int initial_xid, int xid_stride)
{
	dirpath mountdir=discard_const(export);
	struct nfsio *nfsio;
	mountres3 *mountres;
	fhandle3 *fh;
        struct sockaddr_in sin;
	int ret;

	nfsio = malloc(sizeof(struct nfsio));
	if (nfsio == NULL) {
		fprintf(stderr, "Failed to malloc nfsio\n");
		return NULL;
	}
	bzero(nfsio, sizeof(struct nfsio));

	nfsio->s          = -1;
	nfsio->xid        = initial_xid;
	nfsio->xid_stride = xid_stride;

	/*
	 * set up the MOUNT client. If we are running as root, we get
	 * a port <1024 by default. If we are not root, we can not
	 * bind to these ports, so the server must be in "insecure"
	 * mode.
	 */
	memset(&sin, 0, sizeof(sin));
	sin.sin_port = 0;
	sin.sin_family = PF_INET;
	if (inet_aton(server, &sin.sin_addr) == 0) {
		fprintf(stderr, "Invalid address '%s'\n", server);
		nfsio_disconnect(nfsio);
		return NULL;
	}

	if (!strcmp(protocol, "tcp")) {
		nfsio->s = RPC_ANYSOCK;
		nfsio->clnt = clnttcp_create(&sin, MOUNT_PROGRAM, MOUNT_V3, &nfsio->s, 17*1024*1024, 17*1024*1024);
	} else {
		struct timeval wait;

		wait.tv_sec  = 5;
		wait.tv_usec = 0;
		nfsio->s = RPC_ANYSOCK;
		nfsio->clnt = clntudp_create(&sin, MOUNT_PROGRAM, MOUNT_V3, wait, &nfsio->s);
	}

	if (nfsio->clnt == NULL) {
		printf("ERROR: failed to connect to MOUNT daemon on %s\n", server);
		nfsio_disconnect(nfsio);
		return NULL;
	}
	nfsio->clnt->cl_auth = authunix_create_default();

	mountres=mountproc3_mnt_3(&mountdir, nfsio->clnt);
	if (mountres == NULL) {
		printf("ERROR: failed to call the MNT procedure\n");
		nfsio_disconnect(nfsio);
		return NULL;
	}
	if (mountres->fhs_status != MNT3_OK) {
		printf("ERROR: Server returned error %d when trying to MNT\n",mountres->fhs_status);
		nfsio_disconnect(nfsio);
		return NULL;
	}

	fh = &mountres->mountres3_u.mountinfo.fhandle;
	insert_fhandle(nfsio, "/",
			      fh->fhandle3_val,
			      fh->fhandle3_len,
			      0);


	/* we dont need the mount client any more */
	clnt_destroy(nfsio->clnt);
	nfsio->clnt = NULL;
	close(nfsio->s);
	nfsio->s = -1;


	/*
	 * set up the NFS client. If we are running as root, we get
	 * a port <1024 by default. If we are not root, we can not
	 * bind to these ports, so the server must be in "insecure"
	 * mode.
	 */
	memset(&sin, 0, sizeof(sin));
	sin.sin_port = 0;
	sin.sin_family = PF_INET;
	if (inet_aton(server, &sin.sin_addr) == 0) {
		fprintf(stderr, "Invalid address '%s'\n", server);
		nfsio_disconnect(nfsio);
		return NULL;
	}

	if (!strcmp(protocol, "tcp")) {
		nfsio->s = RPC_ANYSOCK;
		nfsio->clnt = clnttcp_create(&sin, NFS_PROGRAM, NFS_V3, &nfsio->s, 17*1024*1024, 17*1024*1024);
	} else {
		struct timeval wait;

		wait.tv_sec  = 5;
		wait.tv_usec = 0;
		nfsio->s = RPC_ANYSOCK;
		nfsio->clnt = clntudp_create(&sin, NFS_PROGRAM, NFS_V3, wait, &nfsio->s);
	}

	if (nfsio->clnt == NULL) {
		fprintf(stderr, "Failed to initialize nfs client structure\n");
		nfsio_disconnect(nfsio);
		return NULL;
	}
	nfsio->clnt->cl_auth = authunix_create_default();

	return nfsio;
}


nfsstat3 nfsio_getattr(struct nfsio *nfsio, const char *name, fattr3 *attributes)
{
	struct GETATTR3args GETATTR3args;
	struct GETATTR3res *GETATTR3res;
	data_t *fh;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_getattr\n");
		return NFS3ERR_SERVERFAULT;
	}

	GETATTR3args.object.data.data_len = fh->dsize;
	GETATTR3args.object.data.data_val = discard_const(fh->dptr);

	set_new_xid(nfsio);
	GETATTR3res = nfsproc3_getattr_3(&GETATTR3args, nfsio->clnt);

	if (GETATTR3res == NULL) {
		fprintf(stderr, "nfsproc3_getattr_3 failed in getattr\n");
		return NFS3ERR_SERVERFAULT;
	}

	if (GETATTR3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_getattr_3 failed in getattr. status:%d\n", GETATTR3res->status);
		return GETATTR3res->status;
	}

	if (attributes) {
		memcpy(attributes, &GETATTR3res->GETATTR3res_u.resok.obj_attributes, sizeof(fattr3));
	}

	return NFS3_OK;
}

nfsstat3 nfsio_lookup(struct nfsio *nfsio, const char *name, fattr3 *attributes)
{

	struct LOOKUP3args LOOKUP3args;
	struct LOOKUP3res *LOOKUP3res;
	char *tmp_name = NULL;
	int ret = NFS3_OK;
	data_t *fh;
	char *ptr;

	tmp_name = strdup(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_lookup\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_lookup\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle for '%s' in nfsio_lookup\n", tmp_name);
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	LOOKUP3args.what.dir.data.data_len = fh->dsize;
	LOOKUP3args.what.dir.data.data_val = discard_const(fh->dptr);
	LOOKUP3args.what.name = ptr;

	set_new_xid(nfsio);
	LOOKUP3res = nfsproc3_lookup_3(&LOOKUP3args, nfsio->clnt);

	if (LOOKUP3res == NULL) {
		fprintf(stderr, "nfsproc3_lookup_3 failed in lookup\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (LOOKUP3res->status != NFS3_OK) {
		ret = LOOKUP3res->status;
		goto finished;
	}

	insert_fhandle(nfsio, name, 
			LOOKUP3res->LOOKUP3res_u.resok.object.data.data_val,
			LOOKUP3res->LOOKUP3res_u.resok.object.data.data_len,
			LOOKUP3res->LOOKUP3res_u.resok.obj_attributes.post_op_attr_u.attributes.size);

	free(LOOKUP3res->LOOKUP3res_u.resok.object.data.data_val);

	if (attributes) {
		memcpy(attributes, &LOOKUP3res->LOOKUP3res_u.resok.obj_attributes.post_op_attr_u.attributes, sizeof(fattr3));
	}

finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}


nfsstat3 nfsio_access(struct nfsio *nfsio, const char *name, uint32 desired, uint32 *access)
{

	struct ACCESS3args ACCESS3args;
	struct ACCESS3res *ACCESS3res;
	data_t *fh;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_access\n");
		return NFS3ERR_SERVERFAULT;
	}

	ACCESS3args.object.data.data_val = discard_const(fh->dptr);
	ACCESS3args.object.data.data_len = fh->dsize;
	ACCESS3args.access = desired;

	set_new_xid(nfsio);
	ACCESS3res = nfsproc3_access_3(&ACCESS3args, nfsio->clnt);

	if (ACCESS3res == NULL) {
		fprintf(stderr, "nfsproc3_access_3 failed in access\n");
		return NFS3ERR_SERVERFAULT;
	}

	if (ACCESS3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_access_3 failed. status:%d\n", 
ACCESS3res->status);
		return ACCESS3res->status;
	}

	if (access) {
		*access = ACCESS3res->ACCESS3res_u.resok.access;
	}

	return NFS3_OK;
}



nfsstat3 nfsio_create(struct nfsio *nfsio, const char *name)
{

	struct CREATE3args CREATE3args;
	struct CREATE3res *CREATE3res;
	char *tmp_name = NULL;
	data_t *fh;
	char *ptr;
	int ret = NFS3_OK;

	tmp_name = strdup(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_create\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_create\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_create\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	CREATE3args.where.dir.data.data_len  = fh->dsize;
	CREATE3args.where.dir.data.data_val  = discard_const(fh->dptr);
	CREATE3args.where.name               = ptr;

	CREATE3args.how.mode = UNCHECKED;
	CREATE3args.how.createhow3_u.obj_attributes.mode.set_it  = TRUE;
	CREATE3args.how.createhow3_u.obj_attributes.mode.set_mode3_u.mode    = 0777;
	CREATE3args.how.createhow3_u.obj_attributes.uid.set_it   = TRUE;
	CREATE3args.how.createhow3_u.obj_attributes.uid.set_uid3_u.uid      = 0;
	CREATE3args.how.createhow3_u.obj_attributes.gid.set_it   = TRUE;
	CREATE3args.how.createhow3_u.obj_attributes.gid.set_gid3_u.gid      = 0;
	CREATE3args.how.createhow3_u.obj_attributes.size.set_it  = FALSE;
	CREATE3args.how.createhow3_u.obj_attributes.atime.set_it = FALSE;
	CREATE3args.how.createhow3_u.obj_attributes.mtime.set_it = FALSE;

	set_new_xid(nfsio);
	CREATE3res = nfsproc3_create_3(&CREATE3args, nfsio->clnt);

	if (CREATE3res == NULL) {
		fprintf(stderr, "nfsproc3_create_3 failed in nfsio_create\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (CREATE3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_create_3 failed in nfsio_create. status:%d\n", CREATE3res->status);
		ret = CREATE3res->status;
		goto finished;
	}


	insert_fhandle(nfsio, name, 
		CREATE3res->CREATE3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
		CREATE3res->CREATE3res_u.resok.obj.post_op_fh3_u.handle.data.data_len,
		0 /*qqq*/
	);


finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}

nfsstat3 nfsio_remove(struct nfsio *nfsio, const char *name)
{

	struct REMOVE3args REMOVE3args;
	struct REMOVE3res *REMOVE3res;
	int ret = NFS3_OK;
	char *tmp_name = NULL;
	data_t *fh;
	char *ptr;

	tmp_name = strdup(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_remove\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_remove\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_remove\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}


	REMOVE3args.object.dir.data.data_len  = fh->dsize;
	REMOVE3args.object.dir.data.data_val  = discard_const(fh->dptr);
	REMOVE3args.object.name               = ptr;

	set_new_xid(nfsio);
	REMOVE3res = nfsproc3_remove_3(&REMOVE3args, nfsio->clnt);

	if (REMOVE3res == NULL) {
		fprintf(stderr, "nfsproc3_remove_3 failed in nfsio_remove\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (REMOVE3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_remove_3 failed in nfsio_remove. status:%d\n", REMOVE3res->status);
		ret = REMOVE3res->status;
		goto finished;
	}


	delete_fhandle(nfsio, name);


finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}


nfsstat3 nfsio_write(struct nfsio *nfsio, const char *name, char *buf, uint64_t offset, int len, int stable)
{
	struct WRITE3args WRITE3args;
	struct WRITE3res *WRITE3res;
	int ret = NFS3_OK;
	data_t *fh;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_write\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	WRITE3args.file.data.data_len = fh->dsize;
	WRITE3args.file.data.data_val = discard_const(fh->dptr);
	WRITE3args.offset             = offset;
	WRITE3args.count              = len;
	WRITE3args.stable             = stable;
	WRITE3args.data.data_len      = len;
	WRITE3args.data.data_val      = buf;


	set_new_xid(nfsio);
	WRITE3res = nfsproc3_write_3(&WRITE3args, nfsio->clnt);

	if (WRITE3res == NULL) {
		fprintf(stderr, "nfsproc3_write_3 failed in nfsio_write\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (WRITE3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_write_3 failed in getattr. status:%d\n", WRITE3res->status);
		ret = WRITE3res->status;
	}

finished:
	return ret;
}

nfsstat3 nfsio_read(struct nfsio *nfsio, const char *name, char *buf, uint64_t offset, int len, int *count, int *eof)
{
	struct READ3args READ3args;
	struct READ3res *READ3res;
	int ret = NFS3_OK;
	data_t *fh;
	off_t size;

	fh = lookup_fhandle(nfsio, name, &size);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_read\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (offset >= size && size > 0) {
		offset = offset % size;
 	}
	if (offset+len >= size) {
		offset = 0;
	}

	READ3args.file.data.data_len = fh->dsize;
	READ3args.file.data.data_val = discard_const(fh->dptr);
	READ3args.offset             = offset;
	READ3args.count              = len;

	set_new_xid(nfsio);
	READ3res = nfsproc3_read_3(&READ3args, nfsio->clnt);

	if (READ3res == NULL) {
		fprintf(stderr, "nfsproc3_read_3 failed in nfsio_read\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (READ3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_read_3 failed in nfsio_read. status:%d\n", READ3res->status);
		ret = READ3res->status;
		goto finished;
	}

	if (count) {
		*count = READ3res->READ3res_u.resok.count;
	}
	if (eof) {
		*eof = READ3res->READ3res_u.resok.eof;
	}
	if (buf) {
		memcpy(buf, READ3res->READ3res_u.resok.data.data_val,
			READ3res->READ3res_u.resok.count);
	}
	free(READ3res->READ3res_u.resok.data.data_val);
	READ3res->READ3res_u.resok.data.data_val = NULL;

finished:
	return ret;
}


nfsstat3 nfsio_commit(struct nfsio *nfsio, const char *name)
{
	struct COMMIT3args COMMIT3args;
	struct COMMIT3res *COMMIT3res;	
	int ret = NFS3_OK;
	data_t *fh;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_commit\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	COMMIT3args.file.data.data_len = fh->dsize;
	COMMIT3args.file.data.data_val = discard_const(fh->dptr);
	COMMIT3args.offset             = 0;
	COMMIT3args.count              = 0;


	set_new_xid(nfsio);
	COMMIT3res = nfsproc3_commit_3(&COMMIT3args, nfsio->clnt);

	if (COMMIT3res == NULL) {
		fprintf(stderr, "nfsproc3_commit_3 failed in nfsio_commit\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (COMMIT3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_commit_3 failed in nfsio_commit. status:%d\n", COMMIT3res->status);
		ret = COMMIT3res->status;
		goto finished;
	}

finished:
	return ret;
}

nfsstat3 nfsio_fsinfo(struct nfsio *nfsio)
{
	struct FSINFO3args FSINFO3args;
	struct FSINFO3res *FSINFO3res;
	data_t *fh;

	fh = lookup_fhandle(nfsio, "/", NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_fsinfo\n");
		return NFS3ERR_SERVERFAULT;
	}

	FSINFO3args.fsroot.data.data_len = fh->dsize;
	FSINFO3args.fsroot.data.data_val = discard_const(fh->dptr);

	set_new_xid(nfsio);
	FSINFO3res = nfsproc3_fsinfo_3(&FSINFO3args, nfsio->clnt);

	if (FSINFO3res == NULL) {
		fprintf(stderr, "nfsproc3_fsinfo_3 failed in nfsio_fsinfo\n");
		return NFS3ERR_SERVERFAULT;
	}

	if (FSINFO3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_fsinfo_3 failed in nfsio_fsinfo. status:%d\n", FSINFO3res->status);
		return FSINFO3res->status;
	}

	return NFS3_OK;
}


nfsstat3 nfsio_fsstat(struct nfsio *nfsio)
{
	struct FSSTAT3args FSSTAT3args;
	struct FSSTAT3res *FSSTAT3res;
	data_t *fh;

	fh = lookup_fhandle(nfsio, "/", NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_fsstat\n");
		return NFS3ERR_SERVERFAULT;
	}

	FSSTAT3args.fsroot.data.data_len = fh->dsize;
	FSSTAT3args.fsroot.data.data_val = discard_const(fh->dptr);

	set_new_xid(nfsio);
	FSSTAT3res = nfsproc3_fsstat_3(&FSSTAT3args, nfsio->clnt);

	if (FSSTAT3res == NULL) {
		fprintf(stderr, "nfsproc3_fsstat_3 failed in nfsio_fsstat\n");
		return NFS3ERR_SERVERFAULT;
	}

	if (FSSTAT3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_fsstat_3 failed in nfsio_fsstat. status:%d\n", FSSTAT3res->status);
		return FSSTAT3res->status;
	}

	return NFS3_OK;
}

nfsstat3 nfsio_pathconf(struct nfsio *nfsio, char *name)
{
	struct PATHCONF3args PATHCONF3args;
	struct PATHCONF3res *PATHCONF3res;
	data_t *fh;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_pathconf\n");
		return NFS3ERR_SERVERFAULT;
	}

	PATHCONF3args.object.data.data_len = fh->dsize;
	PATHCONF3args.object.data.data_val = discard_const(fh->dptr);

	set_new_xid(nfsio);
	PATHCONF3res = nfsproc3_pathconf_3(&PATHCONF3args, nfsio->clnt);

	if (PATHCONF3res == NULL) {
		fprintf(stderr, "nfsproc3_pathconf_3 failed in nfsio_pathconf\n");
		return NFS3ERR_SERVERFAULT;
	}

	if (PATHCONF3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_pathconf_3 failed in nfsio_pathconf. status:%d\n", PATHCONF3res->status);
		return PATHCONF3res->status;
	}

	return NFS3_OK;
}


nfsstat3 nfsio_symlink(struct nfsio *nfsio, const char *old, const char *new)
{

	struct SYMLINK3args SYMLINK3args;
	struct SYMLINK3res *SYMLINK3res;
	int ret = NFS3_OK;
	char *tmp_name = NULL;
	data_t *fh;
	char *ptr;

	tmp_name = strdup(old);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_symlink\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_symlink\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_symlink\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}


	SYMLINK3args.where.dir.data.data_len  = fh->dsize;
	SYMLINK3args.where.dir.data.data_val  = discard_const(fh->dptr);
	SYMLINK3args.where.name		      = ptr;

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


	set_new_xid(nfsio);
	SYMLINK3res = nfsproc3_symlink_3(&SYMLINK3args, nfsio->clnt);

	if (SYMLINK3res == NULL) {
		fprintf(stderr, "nfsproc3_symlink_3 failed in nfsio_symlink\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (SYMLINK3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_symlink_3 failed in nfsio_symlink. status:%d\n", SYMLINK3res->status);
		ret = SYMLINK3res->status;
		goto finished;
	}


	insert_fhandle(nfsio, old, 
		SYMLINK3res->SYMLINK3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
		SYMLINK3res->SYMLINK3res_u.resok.obj.post_op_fh3_u.handle.data.data_len,
		0 /*qqq*/
	);


finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}


nfsstat3 nfsio_link(struct nfsio *nfsio, const char *old, const char *new)
{

	struct LINK3args LINK3args;
	struct LINK3res *LINK3res;
	int ret = NFS3_OK;
	char *tmp_name = NULL;
	data_t *fh, *new_fh;
	char *ptr;

	tmp_name = strdup(old);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_link\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_link\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_link\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}


	new_fh = lookup_fhandle(nfsio, new, NULL);
	if (new_fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_link\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}


	LINK3args.file.data.data_len  = new_fh->dsize;
	LINK3args.file.data.data_val  = discard_const(new_fh->dptr);


	LINK3args.link.dir.data.data_len  = fh->dsize;
	LINK3args.link.dir.data.data_val  = discard_const(fh->dptr);
	LINK3args.link.name	          = ptr;

	set_new_xid(nfsio);
	LINK3res = nfsproc3_link_3(&LINK3args, nfsio->clnt);

	if (LINK3res == NULL) {
		fprintf(stderr, "nfsproc3_link_3 failed in nfsio_link\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (LINK3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_link_3 failed in nfsio_link. status:%d\n", LINK3res->status);
		ret = LINK3res->status;
		goto finished;
	}


//	insert_fhandle(nfsio, old, 
//		LINK3res->LINK3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
//		LINK3res->LINK3res_u.resok.obj.post_op_fh3_u.handle.data.data_len);


finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}



nfsstat3 nfsio_readlink(struct nfsio *nfsio, char *name, char **link_name)
{
	struct READLINK3args READLINK3args;
	struct READLINK3res *READLINK3res;
	data_t *fh;

	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle in nfsio_readlink\n");
		return NFS3ERR_SERVERFAULT;
	}


	READLINK3args.symlink.data.data_len  = fh->dsize;
	READLINK3args.symlink.data.data_val  = discard_const(fh->dptr);

	set_new_xid(nfsio);
	READLINK3res = nfsproc3_readlink_3(&READLINK3args, nfsio->clnt);

	if (READLINK3res == NULL) {
		fprintf(stderr, "nfsproc3_readlink_3 failed in nfsio_readlink\n");
		return NFS3ERR_SERVERFAULT;
	}

	if (READLINK3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_readlink_3 failed in nfsio_readlink. status:%d\n", READLINK3res->status);
		return READLINK3res->status;
	}

	if (link_name) {
		*link_name = strdup(READLINK3res->READLINK3res_u.resok.data);
	}

	return NFS3_OK;
}


nfsstat3 nfsio_rmdir(struct nfsio *nfsio, const char *name)
{

	struct RMDIR3args RMDIR3args;
	struct RMDIR3res *RMDIR3res;
	int ret = NFS3_OK;
	char *tmp_name = NULL;
	data_t *fh;
	char *ptr;

	tmp_name = strdup(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_rmdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_rmdir\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rmdir\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}


	RMDIR3args.object.dir.data.data_len  = fh->dsize;
	RMDIR3args.object.dir.data.data_val  = discard_const(fh->dptr);
	RMDIR3args.object.name               = ptr;

	set_new_xid(nfsio);
	RMDIR3res = nfsproc3_rmdir_3(&RMDIR3args, nfsio->clnt);

	if (RMDIR3res == NULL) {
		fprintf(stderr, "nfsproc3_rmdir_3 failed in nfsio_rmdir\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (RMDIR3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_rmdir_3(%s) failed in nfsio_rmdir. status:%s(%d)\n", name, nfs_error(RMDIR3res->status), RMDIR3res->status);
		ret = RMDIR3res->status;
		goto finished;
	}


	delete_fhandle(nfsio, name);


finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}



nfsstat3 nfsio_mkdir(struct nfsio *nfsio, const char *name)
{

	struct MKDIR3args MKDIR3args;
	struct MKDIR3res *MKDIR3res;
	int ret = NFS3_OK;
	char *tmp_name = NULL;
	data_t *fh;
	char *ptr;

	tmp_name = strdup(name);
	if (tmp_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_mkdir\n");
		return NFS3ERR_SERVERFAULT;
	}

	ptr = rindex(tmp_name, '/');
	if (ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_mkdir\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*ptr = 0;
	ptr++;

	fh = lookup_fhandle(nfsio, tmp_name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_mkdir\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	MKDIR3args.where.dir.data.data_len  = fh->dsize;
	MKDIR3args.where.dir.data.data_val  = discard_const(fh->dptr);
	MKDIR3args.where.name               = ptr;

	MKDIR3args.attributes.mode.set_it  = TRUE;
	MKDIR3args.attributes.mode.set_mode3_u.mode    = 0777;
	MKDIR3args.attributes.uid.set_it   = TRUE;
	MKDIR3args.attributes.uid.set_uid3_u.uid      = 0;
	MKDIR3args.attributes.gid.set_it   = TRUE;
	MKDIR3args.attributes.gid.set_gid3_u.gid      = 0;
	MKDIR3args.attributes.size.set_it  = FALSE;
	MKDIR3args.attributes.atime.set_it = FALSE;
	MKDIR3args.attributes.mtime.set_it = FALSE;

	set_new_xid(nfsio);
	MKDIR3res = nfsproc3_mkdir_3(&MKDIR3args, nfsio->clnt);

	if (MKDIR3res == NULL) {
		fprintf(stderr, "nfsproc3_mkdir_3 failed in nfsio_mkdir\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (MKDIR3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_mkdir_3(%s) failed in nfsio_mkdir. status:%s(%d)\n", name, nfs_error(MKDIR3res->status), MKDIR3res->status);
		ret = MKDIR3res->status;
		goto finished;
	}

	insert_fhandle(nfsio, name, 
		MKDIR3res->MKDIR3res_u.resok.obj.post_op_fh3_u.handle.data.data_val,
		MKDIR3res->MKDIR3res_u.resok.obj.post_op_fh3_u.handle.data.data_len,
		0 /*qqq*/
	);

finished:
	if (tmp_name) {
		free(tmp_name);
	}
	return ret;
}


nfsstat3 nfsio_readdirplus(struct nfsio *nfsio, const char *name, nfs3_dirent_cb cb, void *private_data)
{
	struct READDIRPLUS3args READDIRPLUS3args;
	struct READDIRPLUS3res *READDIRPLUS3res;
	int ret = NFS3_OK;
	data_t *fh;
	entryplus3 *e, *last_e = NULL;
	entryplus3 *entries = NULL;
	entryplus3 *new_entry;
	char *dir = NULL;

	dir = strdup(name);
	while(strlen(dir)){
		if(dir[strlen(dir)-1] != '/'){
			break;
		}
		dir[strlen(dir)-1] = 0;
	}
 
	fh = lookup_fhandle(nfsio, name, NULL);
	if (fh == NULL) {
		fprintf(stderr, "failed to fetch handle for '%s' in nfsio_readdirplus\n", name);
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	READDIRPLUS3args.dir.data.data_len = fh->dsize;
	READDIRPLUS3args.dir.data.data_val = discard_const(fh->dptr);
	READDIRPLUS3args.cookie            = 0;
	bzero(&READDIRPLUS3args.cookieverf, NFS3_COOKIEVERFSIZE);
	READDIRPLUS3args.dircount          = 6000;
	READDIRPLUS3args.maxcount          = 8192;

again:
	set_new_xid(nfsio);
	READDIRPLUS3res = nfsproc3_readdirplus_3(&READDIRPLUS3args, nfsio->clnt);

	if (READDIRPLUS3res == NULL) {
		fprintf(stderr, "nfsproc3_readdirplus_3 failed in readdirplus\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (READDIRPLUS3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_readdirplus_3 failed in readdirplus. status:%d\n", READDIRPLUS3res->status);
		ret = READDIRPLUS3res->status;
		goto finished;
	}

	for(e = READDIRPLUS3res->READDIRPLUS3res_u.resok.reply.entries;e;e=e->nextentry){
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

		last_e = e;

		asprintf(&new_name, "%s/%s", dir, e->name);
		insert_fhandle(nfsio, new_name, 
			e->name_handle.post_op_fh3_u.handle.data.data_val,
			e->name_handle.post_op_fh3_u.handle.data.data_len,
			0 /*qqq*/
		);
		free(new_name);

		new_entry = malloc(sizeof(entryplus3));
		new_entry->name = strdup(e->name);
		new_entry->name_attributes.post_op_attr_u.attributes.type = e->name_attributes.post_op_attr_u.attributes.type;
		new_entry->nextentry = entries;
		entries = new_entry;
	}	

	if (READDIRPLUS3res->READDIRPLUS3res_u.resok.reply.eof == 0) {
		if (READDIRPLUS3args.cookie == 0) {
			memcpy(&READDIRPLUS3args.cookieverf, 
			&READDIRPLUS3res->READDIRPLUS3res_u.resok.cookieverf,
			NFS3_COOKIEVERFSIZE);		
		}

		READDIRPLUS3args.cookie = last_e->cookie;

		goto again;
	}

	/* we have read all entries, now invoke the callback for all of them */
	while (entries != NULL) {
		e = entries;
		entries = entries->nextentry;

		if (cb) {
			cb(e, private_data);
		}
	
		free(e->name);
		free(e);
	}

finished:
	if (dir) {
		free(dir);
	}
	return ret;
}


nfsstat3 nfsio_rename(struct nfsio *nfsio, const char *old, const char *new)
{

	struct RENAME3args RENAME3args;
	struct RENAME3res *RENAME3res;
	int ret = NFS3_OK;
	char *tmp_old_name = NULL;
	char *tmp_new_name = NULL;
	data_t *old_fh, *new_fh;
	char *old_ptr, *new_ptr;

	tmp_old_name = strdup(old);
	if (tmp_old_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	old_ptr = rindex(tmp_old_name, '/');
	if (old_ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*old_ptr = 0;
	old_ptr++;

	old_fh = lookup_fhandle(nfsio, tmp_old_name, NULL);
	if (old_fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	tmp_new_name = strdup(new);
	if (tmp_new_name == NULL) {
		fprintf(stderr, "failed to strdup name in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	new_ptr = rindex(tmp_new_name, '/');
	if (new_ptr == NULL) {	
		fprintf(stderr, "name did not contain '/' in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	*new_ptr = 0;
	new_ptr++;

	new_fh = lookup_fhandle(nfsio, tmp_new_name, NULL);
	if (new_fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	RENAME3args.from.dir.data.data_len  = old_fh->dsize;
	RENAME3args.from.dir.data.data_val  = discard_const(old_fh->dptr);
	RENAME3args.from.name		    = old_ptr;

	RENAME3args.to.dir.data.data_len  = new_fh->dsize;
	RENAME3args.to.dir.data.data_val  = discard_const(new_fh->dptr);
	RENAME3args.to.name		  = new_ptr;


	set_new_xid(nfsio);
	RENAME3res = nfsproc3_rename_3(&RENAME3args, nfsio->clnt);

	if (RENAME3res == NULL) {
		fprintf(stderr, "nfsproc3_rename_3 failed in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}

	if (RENAME3res->status != NFS3_OK) {
		fprintf(stderr, "nfsproc3_rename_3 failed in nfsio_rename. status:%d\n", RENAME3res->status);
		ret = RENAME3res->status;
		goto finished;
	}


	old_fh = lookup_fhandle(nfsio, old, NULL);
	if (old_fh == NULL) {
		fprintf(stderr, "failed to fetch parent handle in nfsio_rename\n");
		ret = NFS3ERR_SERVERFAULT;
		goto finished;
	}


	insert_fhandle(nfsio, new, old_fh->dptr, old_fh->dsize, 0 /*qqq*/);
	delete_fhandle(nfsio, old);


finished:
	if (tmp_old_name) {
		free(tmp_old_name);
	}
	if (tmp_new_name) {
		free(tmp_new_name);
	}
	return ret;
}

