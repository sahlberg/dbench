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
#include "dbench.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdint.h>
#include <libsmbclient.h>

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

static char *smb_domain;
static char *smb_user;
static char *smb_password;
static char *smb_server;
static char *smb_share;

typedef struct _data_t {
	const char *dptr;
	int dsize;
} data_t;

typedef struct _smb_handle_t {
	int fd;
} smb_handle_t;

/* a tree to store all open handles, indexed by path */
typedef struct _tree_t {
	data_t path;
	smb_handle_t handle;
	struct _tree_t *parent;
	struct _tree_t *left;
	struct _tree_t *right;
} tree_t;

static tree_t *open_files;


static tree_t *find_path(tree_t *tree, const char *path)
{
	int i;

	if (tree == NULL) {
		return NULL;
	}

	i = strcmp(path, tree->path.dptr);
	if (i == 0) {
		return tree;
	}
	if (i < 0) {
		return find_path(tree->left, path);
	}

	return find_path(tree->right, path);
}

static smb_handle_t *lookup_path(const char *path)
{
	tree_t *t;

	t = find_path(open_files, path);
	if (t == NULL) {
		return NULL;
	}

	return &t->handle;
}

static void free_node(tree_t *t)
{
	free(discard_const(t->path.dptr));
	free(t);
}

static void delete_path(const char *path)
{
	tree_t *t;

	t = find_path(open_files, path);
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
			open_files = tmp_tree;
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
			open_files = tmp_tree;
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
		open_files = NULL;
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

static void insert_path(const char *path, smb_handle_t *hnd)
{
	tree_t *tmp_t;
	tree_t *t;
	int i;

	tmp_t = find_path(open_files, path);
	if (tmp_t != NULL) {
		delete_path(path);
	}

	t = malloc(sizeof(tree_t));
	if (t == NULL) {
		fprintf(stderr, "MALLOC failed to allocate tree_t in insert_fhandle\n");
		exit(10);
	}

	t->path.dptr = strdup(path);
	if (t->path.dptr == NULL) {
		fprintf(stderr, "STRDUP failed to allocate key in insert_fhandle\n");
		exit(10);
	}
	t->path.dsize = strlen(path);

	t->handle = *hnd;

	t->left   = NULL;
	t->right  = NULL;
	t->parent = NULL;

	if (open_files == NULL) {
		open_files = t;
		return;
	}

	tmp_t = open_files;
again:
	i = strcmp(t->path.dptr, tmp_t->path.dptr);
	if (i == 0) {
		tmp_t->handle = t->handle;
		free(discard_const(t->path.dptr));
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





struct smb_child {
	SMBCCTX *ctx;
};

static void failed(struct child_struct *child)
{
	child->failed = 1;
	fprintf(stderr, "ERROR: child %d failed at line %d\n", child->id, child->line);
	exit(1);
}

static int check_status(int ret, const char *status)
{
	if (!strcmp(status, "*")) {
		return 0;
	}

	if ((!strcmp(status, "SUCCESS")) && (ret == 0)) {
		return 0;
	}

	if ((!strcmp(status, "ERROR")) && (ret != 0)) {
		return 0;
	}

	return 1;
}


void smb_auth_fn(const char *server, const char *share, char *wrkgrp, int wrkgrplen, char *user, int userlen, char *passwd, int passwdlen)
{
	(void)server;
	(void)share;

	if (smb_domain != NULL) {
		strncpy(wrkgrp, smb_domain, wrkgrplen - 1); wrkgrp[wrkgrplen - 1] = 0;
	}
	strncpy(user, smb_user, userlen - 1); user[userlen - 1] = 0;
	if (smb_password != NULL) {
		strncpy(passwd, smb_password, passwdlen - 1); passwd[passwdlen - 1] = 0;
	}
}

static int smb_init(void)
{
	SMBCCTX *ctx;
	char *tmp;
	int ret;
	char *str;

	if (options.smb_share == NULL) {
		fprintf(stderr, "You must specify --smb-share=<share> with the \"smb\" backend.\n");
		return 1;
	}
	if (options.smb_share[0] != '/' || options.smb_share[1] != '/') {
		fprintf(stderr, "--smb-share Must be of the form //SERVER/SHARE[/PATH]\n");
		return 1;
	}
	smb_server = strdup(options.smb_share+2);
	tmp = index(smb_server, '/');
	if (tmp == NULL) {
		fprintf(stderr, "--smb-share Must be of the form //SERVER/SHARE[/PATH]\n");
		return 1;
	}
	*tmp = '\0';
	smb_share = tmp+1;		

	if (options.smb_user == NULL) {
		fprintf(stderr, "You must specify --smb-user=[<domain>/]<user>%%<password> with the \"smb\" backend.\n");
		return 1;
	}

	smb_domain = strdup(options.smb_user);
	tmp = index(smb_domain, '/');
	if (tmp == NULL) {
		smb_user = smb_domain;
		smb_domain = NULL;
	} else {
		smb_user = tmp+1;
		*tmp = '\0';
	}
	tmp = index(smb_user, '%');
	if (tmp == NULL) {
		smb_password = NULL;
	} else {
		smb_password = tmp+1;
		*tmp = '\0';
	}

	ctx = smbc_new_context();
	if (ctx == NULL) {
		fprintf(stderr, "Could not allocate SMB Context\n");
		return 1;
	}
	ctx->debug = 0;
	ctx->callbacks.auth_fn = smb_auth_fn;

	if (!smbc_init_context(ctx)) {
		smbc_free_context(ctx, 0);
		fprintf(stderr, "failed to initialize context\n");
		return 1;
	}
	smbc_set_context(ctx);

	asprintf(&str, "smb://%s/%s", smb_server, smb_share);
	ret = smbc_opendir(str);
	free(str);

	if (ret == -1) {
		fprintf(stderr, "Failed to access //%s/%s\n", smb_server, smb_share);
		return 1;
	}

	smbc_free_context(ctx, 1);
	return 0;
}

static void smb_setup(struct child_struct *child)
{
	struct smb_child *ctx;

	ctx = malloc(sizeof(struct smb_child));
	if (ctx == NULL) {
		fprintf(stderr, "Failed to malloc child ctx\n");
		exit(10);
	}
	child->private =ctx;

	ctx->ctx = smbc_new_context();
	if (ctx->ctx == NULL) {
		fprintf(stderr, "Could not allocate SMB Context\n");
		exit(10);
	}
	ctx->ctx->debug = 0;
	ctx->ctx->callbacks.auth_fn = smb_auth_fn;

	if (!smbc_init_context(ctx->ctx)) {
		smbc_free_context(ctx->ctx, 0);
		fprintf(stderr, "failed to initialize context\n");
		exit(10);
	}
	smbc_set_context(ctx->ctx);
}

static void smb_mkdir(struct dbench_op *op)
{
	char *str;
	const char *dir;
	int ret;

	dir = op->fname + 2;

	asprintf(&str, "smb://%s/%s/%s", smb_server, smb_share, dir);

	ret = smbc_mkdir(str, 0777);
	free(str);

	if (check_status(ret, op->status)) {
		fprintf(stderr, "[%d] MKDIR \"%s\" failed - expected %s, got %d\n", op->child->line, dir, op->status, ret);
		failed(op->child);
	}
}

static void smb_rmdir(struct dbench_op *op)
{
	char *str;
	const char *dir;
	int ret;

	dir = op->fname + 2;
	asprintf(&str, "smb://%s/%s/%s", smb_server, smb_share, dir);
	ret = smbc_rmdir(str);
	free(str);

	if (check_status(ret, op->status)) {
		fprintf(stderr, "[%d] RMDIR \"%s\" failed - expected %s, got %d\n", op->child->line, dir, op->status, ret);
		failed(op->child);
	}
}

static void smb_open(struct dbench_op *op)
{
	const char *file;
	char *str;
	int flags = 0;
	smb_handle_t hnd;

	if (op->params[0] & 0x01) {
		flags |= O_RDONLY;
	}
	if (op->params[0] & 0x02) {
		flags |= O_WRONLY;
	}
	if (op->params[0] & 0x04) {
		flags |= O_RDWR;
	}
	if (op->params[0] & 0x08) {
		flags |= O_CREAT;
	}
	if (op->params[0] & 0x10) {
		flags |= O_EXCL;
	}
	if (op->params[0] & 0x20) {
		flags |= O_TRUNC;
	}
	if (op->params[0] & 0x40) {
		flags |= O_APPEND;
	}

	file = op->fname + 2;
	asprintf(&str, "smb://%s/%s/%s", smb_server, smb_share, file);

	hnd.fd = smbc_open(str, flags, 0777);
	free(str);

	if (check_status(hnd.fd<0?-1:0, op->status)) {
		fprintf(stderr, "[%d] OPEN \"%s\" failed\n", op->child->line, file);
		failed(op->child);
		return;
	}

	insert_path(file, &hnd);

}

static void smb_close(struct dbench_op *op)
{
	smb_handle_t *hnd;
	const char *file;
	int ret;

	file = op->fname + 2;

	hnd = lookup_path(file);
	if (hnd == NULL) {
		fprintf(stderr, "[%d] CLOSE \"%s\" failed. This file is not open.\n", op->child->line, file);
		failed(op->child);
		return;
	}
		
	ret = smbc_close(hnd->fd);
	delete_path(file);

	if (check_status(ret, op->status)) {
		fprintf(stderr, "[%d] CLOSE \"%s\" failed\n", op->child->line, file);
		failed(op->child);
		return;
	}
}

static void smb_write(struct dbench_op *op)
{
	smb_handle_t *hnd;
	const char *file;
	int ret;
	size_t length;
	off_t offset;
	char garbage[65536];

	offset = op->params[0];
	length = op->params[1];
	if (length > 65536) {
		length = 65536;
	}

	file = op->fname + 2;

	hnd = lookup_path(file);
	if (hnd == NULL) {
		fprintf(stderr, "[%d] WRITE \"%s\" failed. This file is not open.\n", op->child->line, file);
		failed(op->child);
		return;
	}


	smbc_lseek(hnd->fd, offset, SEEK_SET);
	ret = smbc_write(hnd->fd, garbage, length);

	if (check_status(ret==(int)length?0:-1, op->status)) {
		fprintf(stderr, "[%d] WRITE \"%s\" failed\n", op->child->line, file);
		failed(op->child);
		return;
	}
	op->child->bytes += length;
}

static void smb_read(struct dbench_op *op)
{
	smb_handle_t *hnd;
	const char *file;
	int ret;
	size_t length;
	off_t offset;
	char garbage[65536];

	offset = op->params[0];
	length = op->params[1];
	if (length > 65536) {
		length = 65536;
	}

	file = op->fname + 2;

	hnd = lookup_path(file);
	if (hnd == NULL) {
		fprintf(stderr, "[%d] READ \"%s\" failed. This file is not open.\n", op->child->line, file);
		failed(op->child);
		return;
	}


	smbc_lseek(hnd->fd, offset, SEEK_SET);
	ret = smbc_read(hnd->fd, garbage, length);

	if (check_status(ret==(int)length?0:-1, op->status)) {
		fprintf(stderr, "[%d] READ \"%s\" failed\n", op->child->line, file);
		failed(op->child);
		return;
	}
	op->child->bytes += length;
}


static void smb_unlink(struct dbench_op *op)
{
	const char *path;
	char *str;
	int ret;

	path = op->fname + 2;
	asprintf(&str, "smb://%s/%s/%s", smb_server, smb_share, path);

	ret = smbc_unlink(str);
	free(str);

	if (check_status(ret, op->status)) {
		fprintf(stderr, "[%d] UNLINK \"%s\" failed\n", op->child->line, path);
		failed(op->child);
		return;
	}
}

static void recursive_delete_tree(struct dbench_op *op, const char *url)
{
	int dir;
	struct smbc_dirent *dirent;

	dir = smbc_opendir(url);
	if (dir < 0) {
		fprintf(stderr, "[%d] Deltree \"%s\" failed\n", op->child->line, url);
		failed(op->child);
	}
	while((dirent = smbc_readdir(dir))) {
		char *path;

		asprintf(&path, "%s/%s", url, dirent->name);
		if (!strcmp(dirent->name, ".")) {
			continue;
		}
		if (!strcmp(dirent->name, "..")) {
			continue;
		}
		if (dirent->smbc_type == SMBC_DIR) {
			recursive_delete_tree(op, path);
			smbc_rmdir(path);
		} else {
			smbc_unlink(path);
		}
		free(path);
	}
	smbc_closedir(dir);

	return;
}

static void smb_readdir(struct dbench_op *op)
{
	const char *path;
	char *url;
	int dir;

	path = op->fname + 2;
	asprintf(&url, "smb://%s/%s/%s", smb_server, smb_share, path);

	dir = smbc_opendir(url);
	free(url);
	if (dir < 0) {
		fprintf(stderr, "[%d] READDIR \"%s\" failed\n", op->child->line, url);
		failed(op->child);
	}
	smbc_closedir(dir);
}

static void smb_deltree(struct dbench_op *op)
{
	const char *path;
	char *url;

	path = op->fname + 2;
	asprintf(&url, "smb://%s/%s/%s", smb_server, smb_share, path);
	recursive_delete_tree(op, url);
	free(url);
}

static void smb_cleanup(struct child_struct *child)
{
	struct smb_child *ctx = child->private;
	char *url;
	struct dbench_op fake_op;

	asprintf(&url, "smb://%s/%s", smb_server, smb_share);
	recursive_delete_tree(&fake_op, url);
	free(url);

	smbc_free_context(ctx->ctx, 1);
	free(ctx);	
}


	
static struct backend_op ops[] = {
	{ "Deltree",  smb_deltree },
	{ "CLOSE", smb_close },
	{ "MKDIR", smb_mkdir },
	{ "OPEN", smb_open },
	{ "READ", smb_read },
	{ "READDIR", smb_readdir },
	{ "RMDIR", smb_rmdir },
	{ "UNLINK", smb_unlink },
	{ "WRITE", smb_write },
	{ NULL, NULL}
};

struct nb_operations smb_ops = {
	.backend_name = "smbbench",
	.init	      = smb_init,
	.setup 	      = smb_setup,
	.cleanup      = smb_cleanup,
	.ops          = ops
};
