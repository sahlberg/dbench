/* 
   dbench version 1
   Copyright (C) Andrew Tridgell 1999
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

/* This file links against either fileio.c to do operations against a
   local filesystem (making dbench), or sockio.c to issue SMB-like
   command packets over a socket (making tbench).

   So, the pattern of operations and the control structure is the same
   for both benchmarks, but the operations performed are different.
*/

#include "dbench.h"

int line_count=0;

char *client_filename = DATADIR "client.txt";


static int sigsegv(int sig)
{
	char line[200];
	printf("segv at line %d\n", line_count);
	sprintf(line, "/usr/X11R6/bin/xterm -e gdb /proc/%d/exe %d", 
		getpid(), getpid());
	system(line);
	exit(1);
}


FILE * open_client_dump(void)
{
	FILE		*f;

	if ((f = fopen(client_filename, "rt")) != NULL)
		return f;

	fprintf(stderr,
		"dbench: error opening %s: %s", client_filename,
		strerror(errno));

	return NULL;
}

void child_run(int client)
{
	int i;
	char fname[128];
	char line[1024];
	char cname[20];
	FILE *f;
	char *params[20];

	signal(SIGSEGV, sigsegv);

	sprintf(cname,"CLIENT%d", client);

	f = open_client_dump();

	if (!f) {
		perror("client.txt");
		return;
	}

	while (fgets(line, sizeof(line)-1, f)) {
		line_count++;

		line[strlen(line)-1] = 0;

		all_string_sub(line,"CLIENT1", cname);
		all_string_sub(line,"\\", "/");
		all_string_sub(line," /", " ");
		
		for (i=0;i<20;i++) params[i] = "";

		/* parse the command parameters */
		params[0] = strtok(line," ");
		i = 0;
		while (params[i]) params[++i] = strtok(NULL," ");

		params[i] = "";

		if (i < 2) continue;

		if (strcmp(params[1],"REQUEST") == 0) {
			if (!strcmp(params[0],"SMBopenX")) {
				strcpy(fname, params[5]);
			} else if (!strcmp(params[0],"SMBclose")) {
				nb_close(atoi(params[3]));
			} else if (!strcmp(params[0],"SMBmkdir")) {
				nb_mkdir(params[3]);
			} else if (!strcmp(params[0],"CREATE")) {
				nb_create(params[3], atoi(params[5]));
			} else if (!strcmp(params[0],"SMBrmdir")) {
				nb_rmdir(params[3]);
			} else if (!strcmp(params[0],"SMBunlink")) {
				strcpy(fname, params[3]);
			} else if (!strcmp(params[0],"SMBmv")) {
				nb_rename(params[3], params[5]);
			} else if (!strcmp(params[0],"SMBgetatr")) {
				strcpy(fname, params[3]);
			} else if (!strcmp(params[0],"SMBwrite")) {
				nb_write(atoi(params[3]), 
					 atoi(params[5]), atoi(params[7]));
			} else if (!strcmp(params[0],"SMBwritebraw")) {
				nb_write(atoi(params[3]), 
					 atoi(params[7]), atoi(params[5]));
			} else if (!strcmp(params[0],"SMBreadbraw")) {
				nb_read(atoi(params[3]), 
					 atoi(params[7]), atoi(params[5]));
			} else if (!strcmp(params[0],"SMBread")) {
				nb_read(atoi(params[3]), 
					 atoi(params[5]), atoi(params[7]));
			}
		} else {
			if (!strcmp(params[0],"SMBopenX")) {
				if (!strncmp(params[2], "ERR", 3)) continue;
				nb_open(fname, atoi(params[3]), atoi(params[5]));
			} else if (!strcmp(params[0],"SMBgetatr")) {
				if (!strncmp(params[2], "ERR", 3)) continue;
				nb_stat(fname, atoi(params[3]));
			} else if (!strcmp(params[0],"SMBunlink")) {
				if (!strncmp(params[2], "ERR", 3)) continue;
				nb_unlink(fname);
			}
		}
	}
	fclose(f);

	sprintf(fname,"CLIENTS/CLIENT%d", client);
	rmdir(fname);
	rmdir("CLIENTS");

	printf("+");
}
