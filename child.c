/* 
   dbench version 3

   Copyright (C) Andrew Tridgell 1999-2004
   
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

/* This file links against either fileio.c to do operations against a
   local filesystem (making dbench), or sockio.c to issue SMB-like
   command packets over a socket (making tbench).

   So, the pattern of operations and the control structure is the same
   for both benchmarks, but the operations performed are different.
*/

#include "dbench.h"

#define ival(s) strtol(s, NULL, 0)

static void nb_target_rate(struct child_struct *child, double rate)
{
	static double last_bytes;
	static struct timeval last_time;
	double tdelay;

	if (last_bytes == 0) {
		last_bytes = child->bytes;
		last_time = timeval_current();
		return;
	}

	if (rate != 0) {
		tdelay = (child->bytes - last_bytes)/(1.0e6*rate) - timeval_elapsed(&last_time);
	} else {
		tdelay = - timeval_elapsed(&last_time);
	}
	if (tdelay > 0 && rate != 0) {
		msleep(tdelay*1000);
	} else {
		child->max_latency = MAX(child->max_latency, -tdelay);
	}

	last_time = timeval_current();
	last_bytes = child->bytes;
}

static void nb_time_reset(struct child_struct *child)
{
	child->starttime = timeval_current();	
}

static void nb_time_delay(struct child_struct *child, double targett)
{
	double elapsed = timeval_elapsed(&child->starttime);
	if (targett > elapsed) {
		msleep(1000*(targett - elapsed));
	} else if (elapsed - targett > child->max_latency) {
		child->max_latency = MAX(elapsed - targett, child->max_latency);
	}
}



/* run a test that simulates an approximate netbench client load */
void child_run(struct child_struct *child, const char *loadfile)
{
	int i;
	char line[1024];
	char *cname;
	char **sparams, **params;
	char *p;
	const char *status;
	char *fname = NULL;
	char *fname2 = NULL;
	FILE *f = fopen(loadfile, "r");
	pid_t parent = getppid();
	double targett;

	child->line = 0;

	asprintf(&cname,"client%d", child->id);

	sparams = calloc(20, sizeof(char *));
	for (i=0;i<20;i++) {
		sparams[i] = malloc(100);
	}

again:
	nb_time_reset(child);

	while (fgets(line, sizeof(line)-1, f)) {
		params = sparams;

		if (child->done || kill(parent, 0) == -1) {
			goto done;
		}

		child->line++;

		line[strlen(line)-1] = 0;

		all_string_sub(line,"client1", cname);
		all_string_sub(line,"\\", "/");
		all_string_sub(line," /", " ");
		
		p = line;
		for (i=0; 
		     i<19 && next_token(&p, params[i], " ");
		     i++) ;

		params[i][0] = 0;

		if (i < 2 || params[0][0] == '#') continue;

		if (!strncmp(params[0],"SMB", 3)) {
			printf("ERROR: You are using a dbench 1 load file\n");
			exit(1);
		}

		if (i > 0 && isdigit(params[0][0])) {
			targett = strtod(params[0], NULL);
			params++;
			i--;
		} else {
			targett = 0.0;
		}

		if (options.targetrate != 0 || targett == 0.0) {
			nb_target_rate(child, options.targetrate);
		} else {
			nb_time_delay(child, targett);
		}
		child->lasttime = timeval_current();

		if (strncmp(params[i-1], "NT_STATUS_", 10) != 0 &&
		    strncmp(params[i-1], "0x", 2) != 0) {
			printf("Badly formed status at line %d\n", child->line);
			continue;
		}

		if (fname) {
			free(fname);
			fname = NULL;
		}
		if (fname2) {
			free(fname2);
			fname2 = NULL;
		}

		if (i>1 && params[1][0] == '/') {
			asprintf(&fname, "%s%s", child->directory, params[1]);
		}
		if (i>2 && params[2][0] == '/') {
			asprintf(&fname2, "%s%s", child->directory, params[2]);
		}

		status = params[i-1];

		if (!strcmp(params[0],"NTCreateX")) {
			nb_createx(child, fname, ival(params[2]), ival(params[3]), 
				   ival(params[4]), status);
		} else if (!strcmp(params[0],"Close")) {
			nb_close(child, ival(params[1]), status);
		} else if (!strcmp(params[0],"Rename")) {
			nb_rename(child, fname, fname2, status);
		} else if (!strcmp(params[0],"Unlink")) {
			nb_unlink(child, fname, ival(params[2]), status);
		} else if (!strcmp(params[0],"Deltree")) {
			nb_deltree(child, fname);
		} else if (!strcmp(params[0],"Rmdir")) {
			nb_rmdir(child, fname, status);
		} else if (!strcmp(params[0],"Mkdir")) {
			nb_mkdir(child, fname, status);
		} else if (!strcmp(params[0],"QUERY_PATH_INFORMATION")) {
			nb_qpathinfo(child, fname, ival(params[2]), status);
		} else if (!strcmp(params[0],"QUERY_FILE_INFORMATION")) {
			nb_qfileinfo(child, ival(params[1]), ival(params[2]), status);
		} else if (!strcmp(params[0],"QUERY_FS_INFORMATION")) {
			nb_qfsinfo(child, ival(params[1]), status);
		} else if (!strcmp(params[0],"SET_FILE_INFORMATION")) {
			nb_sfileinfo(child, ival(params[1]), ival(params[2]), status);
		} else if (!strcmp(params[0],"FIND_FIRST")) {
			nb_findfirst(child, fname, ival(params[2]), 
				     ival(params[3]), ival(params[4]), status);
		} else if (!strcmp(params[0],"WriteX")) {
			nb_writex(child, ival(params[1]), 
				  ival(params[2]), ival(params[3]), ival(params[4]),
				  status);
		} else if (!strcmp(params[0],"LockX")) {
			nb_lockx(child, ival(params[1]), 
				 ival(params[2]), ival(params[3]), status);
		} else if (!strcmp(params[0],"UnlockX")) {
			nb_unlockx(child, ival(params[1]), 
				 ival(params[2]), ival(params[3]), status);
		} else if (!strcmp(params[0],"ReadX")) {
			nb_readx(child, ival(params[1]), 
				 ival(params[2]), ival(params[3]), ival(params[4]),
				 status);
		} else if (!strcmp(params[0],"Flush")) {
			nb_flush(child, ival(params[1]), status);
		} else if (!strcmp(params[0],"Sleep")) {
			nb_sleep(child, ival(params[1]), status);
		} else {
			printf("[%d] Unknown operation %s\n", child->line, params[0]);
		}
	}

	rewind(f);
	goto again;

done:
	child->cleanup = 1;
	fclose(f);
	nb_cleanup(child);
}
