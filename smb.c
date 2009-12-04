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

#define _GNU_SOURCE
#include <stdio.h>
#undef _GNU_SOURCE

#include <sys/types.h>
#include <sys/socket.h>
#include <stdint.h>
#include <libsmbclient.h>
#include "dbench.h"

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

static char *smb_domain;
static char *smb_user;
static char *smb_password;

struct smb_child {
	SMBCCTX *ctx;
};

void smb_auth_fn(const char *server, const char *share, char *wrkgrp, int wrkgrplen, char *user, int userlen, char *passwd, int passwdlen)
{
	(void)server;
	(void)share;

	if (smb_domain != NULL) {
		strncpy(wrkgrp, smb_domain, wrkgrplen - 1); wrkgrp[wrkgrplen - 1] = 0;
	}
	strncpy(user, smb_user, userlen - 1); user[userlen - 1] = 0;
	strncpy(passwd, smb_password, passwdlen - 1); passwd[passwdlen - 1] = 0;
}

static int smb_init(void)
{
	SMBCCTX *ctx;
	char *tmp;
	int ret;
	char *str;

	if (options.smb_server == NULL) {
		printf("You must specify --smb-server=<server> with the \"smb\" backend.\n");
		return 1;
	}
	if (options.smb_share == NULL) {
		printf("You must specify --smb-share=<share> with the \"smb\" backend.\n");
		return 1;
	}
	if (options.smb_user == NULL) {
		printf("You must specify --smb-user=[<domain>/]<user>%%<password> with the \"smb\" backend.\n");
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
		printf("Could not allocate SMB Context\n");
		return 1;
	}

	smbc_setDebug(ctx, 0);
	smbc_setFunctionAuthData(ctx, smb_auth_fn);

	if (!smbc_init_context(ctx)) {
		smbc_free_context(ctx, 0);
		printf("failed to initialize context\n");
		return 1;
	}
	smbc_set_context(ctx);

	asprintf(&str, "smb://%s/%s", options.smb_server, options.smb_share);
	ret = smbc_opendir(str);
	free(str);

	if (ret == -1) {
		printf("Failed to access //%s/%s\n", options.smb_server, options.smb_share);
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
		printf("Failed to malloc child ctx\n");
		exit(10);
	}
	child->private =ctx;

	ctx->ctx = smbc_new_context();
	if (ctx->ctx == NULL) {
		printf("Could not allocate SMB Context\n");
		exit(10);
	}

	smbc_setDebug(ctx->ctx, 0);
	smbc_setFunctionAuthData(ctx->ctx, smb_auth_fn);

	if (!smbc_init_context(ctx->ctx)) {
		smbc_free_context(ctx->ctx, 0);
		printf("failed to initialize context\n");
		exit(10);
	}
	smbc_set_context(ctx->ctx);
}

static void smb_cleanup(struct child_struct *child)
{
	struct smb_child *ctx = child->private;

	smbc_free_context(ctx->ctx, 1);
	free(ctx);	
}

static void smb_mkdir(struct dbench_op *op)
{
	char *str;
	const char *dir;
	int ret;

	dir = op->fname + 2;

	asprintf(&str, "smb://%s/%s/%s", options.smb_server, options.smb_share, dir);

	ret = smbc_mkdir(str, 0777);

	free(str);
}

static void smb_rmdir(struct dbench_op *op)
{
	char *str;
	const char *dir;
	int ret;

	dir = op->fname + 2;
	asprintf(&str, "smb://%s/%s/%s", options.smb_server, options.smb_share, dir);
	ret = smbc_rmdir(str);

	free(str);
}

static struct backend_op ops[] = {
	{ "Mkdir", smb_mkdir },
	{ "Rmdir", smb_rmdir },
	{ NULL, NULL}
};

struct nb_operations smb_ops = {
	.backend_name = "smbbench",
	.init	      = smb_init,
	.setup 	      = smb_setup,
	.cleanup      = smb_cleanup,
	.ops          = ops
};
