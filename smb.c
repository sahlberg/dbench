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
#include "dbench.h"

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

static int smb_init(void)
{
	if (options.smb_server == NULL) {
		printf("You must specify --smb-server=<server> with the \"smb\" backend.\n");
		return 1;
	}
	if (options.smb_share == NULL) {
		printf("You must specify --smb-share=<share> with the \"smb\" backend.\n");
		return 1;
	}
	if (options.smb_user == NULL) {
		printf("You must specify --smb-user=[<domain>/]<user>%<password> with the \"smb\" backend.\n");
		return 1;
	}

	printf("smb_init\n");


	return 1;
}

static void smb_setup(struct child_struct *child)
{
	printf("smb_setup\n");
}

static void smb_cleanup(struct child_struct *child)
{
	printf("smb_cleanup\n");
}


static struct backend_op ops[] = {
	{ NULL, NULL}
};

struct nb_operations smb_ops = {
	.backend_name = "smbbench",
	.init	      = smb_init,
	.setup 	      = smb_setup,
	.cleanup      = smb_cleanup,
	.ops          = ops
};
