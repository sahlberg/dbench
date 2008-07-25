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
	double tdelay;

	if (child->rate.last_bytes == 0) {
		child->rate.last_bytes = child->bytes;
		child->rate.last_time = timeval_current();
		return;
	}

	if (rate != 0) {
		tdelay = (child->bytes - child->rate.last_bytes)/(1.0e6*rate) - 
			timeval_elapsed(&child->rate.last_time);
	} else {
		tdelay = - timeval_elapsed(&child->rate.last_time);
	}
	if (tdelay > 0 && rate != 0) {
		msleep(tdelay*1000);
	} else {
		child->max_latency = MAX(child->max_latency, -tdelay);
	}

	child->rate.last_time = timeval_current();
	child->rate.last_bytes = child->bytes;
}

static void nb_time_reset(struct child_struct *child)
{
	child->starttime = timeval_current();	
	memset(&child->rate, 0, sizeof(child->rate));
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

static void finish_op(struct child_struct *child, struct op *op)
{
	double t = timeval_elapsed(&child->lasttime);
	op->count++;
	op->total_time += t;
	if (t > op->max_latency) {
		op->max_latency = t;
	}
}

#define OP_LATENCY(opname) finish_op(child, &child->op.op_ ## opname)

/*
  one child operation
 */
static void child_op(struct child_struct *child, char **params, 
		     const char *fname, const char *fname2, const char *status)
{
	child->lasttime = timeval_current();

	if (!strcmp(params[0],"NTCreateX")) {
		if (nb_ops.createx) {
			nb_ops.createx(child, fname, ival(params[2]), ival(params[3]), 
				   ival(params[4]), status);
			OP_LATENCY(NTCreateX);
		} else {
			printf("Operation NTCREATEX is not supported\n");
		}
	} else if (!strcmp(params[0],"Close")) {
		if (nb_ops.close) {
			nb_ops.close(child, ival(params[1]), status);
			OP_LATENCY(Close);
		} else {
			printf("Operation CLOSE is not supported\n");
		}
	} else if (!strcmp(params[0],"Rename")) {
		if (nb_ops.rename) {
			nb_ops.rename(child, fname, fname2, status);
			OP_LATENCY(Rename);
		} else {
			printf("Operation RENAME is not supported\n");
		}
	} else if (!strcmp(params[0],"Unlink")) {
		if (nb_ops.unlink) {
			nb_ops.unlink(child, fname, ival(params[2]), status);
			OP_LATENCY(Unlink);
		} else {
			printf("Operation UNLINK is not supported\n");
		}
	} else if (!strcmp(params[0],"Deltree")) {
		if (nb_ops.deltree) {
			nb_ops.deltree(child, fname);
			OP_LATENCY(Deltree);
		} else {
			printf("Operation DELTREE is not supported\n");
		}
	} else if (!strcmp(params[0],"Rmdir")) {
		if (nb_ops.rmdir) {
			nb_ops.rmdir(child, fname, status);
			OP_LATENCY(Rmdir);
		} else {
			printf("Operation RMDIR is not supported\n");
		}
	} else if (!strcmp(params[0],"Mkdir")) {
		if (nb_ops.mkdir) {
			nb_ops.mkdir(child, fname, status);
			OP_LATENCY(Mkdir);
		} else {
			printf("Operation MKDIR is not supported\n");
		}
	} else if (!strcmp(params[0],"QUERY_PATH_INFORMATION")) {
		if (nb_ops.qpathinfo) {
			nb_ops.qpathinfo(child, fname, ival(params[2]), status);
			OP_LATENCY(Qpathinfo);
		} else {
			printf("Operation QUERY_PATH_INFORMATION is not supported\n");
		}
	} else if (!strcmp(params[0],"QUERY_FILE_INFORMATION")) {
		if (nb_ops.qfileinfo) {
			nb_ops.qfileinfo(child, ival(params[1]), ival(params[2]), status);
			OP_LATENCY(Qfileinfo);
		} else {
			printf("Operation QUERY_FILE_INFORMATION is not supported\n");
		}
	} else if (!strcmp(params[0],"QUERY_FS_INFORMATION")) {
		if (nb_ops.qfsinfo) {
			nb_ops.qfsinfo(child, ival(params[1]), status);
			OP_LATENCY(Qfsinfo);
		} else {
			printf("Operation QUERY_FS_INFORMATION is not supported\n");
		}
	} else if (!strcmp(params[0],"SET_FILE_INFORMATION")) {
		if (nb_ops.sfileinfo) {
			nb_ops.sfileinfo(child, ival(params[1]), ival(params[2]), status);
			OP_LATENCY(Sfileinfo);
		} else {
			printf("Operation SET_FILE_INFORMATION is not supported\n");
		}
	} else if (!strcmp(params[0],"FIND_FIRST")) {
		if (nb_ops.findfirst) {
			nb_ops.findfirst(child, fname, ival(params[2]), 
				     ival(params[3]), ival(params[4]), status);
			OP_LATENCY(Find);
		} else {
			printf("Operation FINDFIRST is not supported\n");
		}
	} else if (!strcmp(params[0],"WriteX")) {
		if (nb_ops.writex) {
			nb_ops.writex(child, ival(params[1]), 
				  ival(params[2]), ival(params[3]), ival(params[4]),
				  status);
			OP_LATENCY(WriteX);
		} else {
			printf("Operation WRITEX is not supported\n");
		}
	} else if (!strcmp(params[0],"LockX")) {
		if (nb_ops.lockx) {
			nb_ops.lockx(child, ival(params[1]), 
				 ival(params[2]), ival(params[3]), status);
			OP_LATENCY(LockX);
		} else {
			printf("Operation LOCKX is not supported\n");
		}
	} else if (!strcmp(params[0],"UnlockX")) {
		if (nb_ops.unlockx) {
			nb_ops.unlockx(child, ival(params[1]), 
				   ival(params[2]), ival(params[3]), status);
			OP_LATENCY(UnlockX);
		} else {
			printf("Operation UNLOCKX is not supported\n");
		}
	} else if (!strcmp(params[0],"ReadX")) {
		if (nb_ops.readx) {
			nb_ops.readx(child, ival(params[1]), 
				 ival(params[2]), ival(params[3]), ival(params[4]),
				 status);
			OP_LATENCY(ReadX);
		} else {
			printf("Operation READX is not supported\n");
		}
	} else if (!strcmp(params[0],"Flush")) {
		if (nb_ops.flush) {
			nb_ops.flush(child, ival(params[1]), status);
			OP_LATENCY(Flush);
		} else {
			printf("Operation FLUSH is not supported\n");
		}
	} else if (!strcmp(params[0],"GETATTR3")) {
		if (nb_ops.getattr3) {
			nb_ops.getattr3(child, fname, status);
			OP_LATENCY(GETATTR3);
		} else {
			printf("Operation GETATTR3 is not supported\n");
		}
	} else if (!strcmp(params[0],"LOOKUP3")) {
		if (nb_ops.getattr3) {
			nb_ops.lookup3(child, fname, status);
			OP_LATENCY(LOOKUP3);
		} else {
			printf("Operation LOOKUP3 is not supported\n");
		}
	} else if (!strcmp(params[0],"CREATE3")) {
		if (nb_ops.create3) {
			nb_ops.create3(child, fname, status);
			OP_LATENCY(CREATE3);
		} else {
			printf("Operation CREATE3 is not supported\n");
		}
	} else if (!strcmp(params[0],"WRITE3")) {
		if (nb_ops.write3) {
			nb_ops.write3(child, fname, ival(params[2]),
				ival(params[3]),
		   		ival(params[4]), status);
			OP_LATENCY(WRITE3);
		} else {
			printf("Operation WRITE3 is not supported\n");
		}
	} else if (!strcmp(params[0],"COMMIT3")) {
		if (nb_ops.commit3) {
			nb_ops.commit3(child, fname, status);
			OP_LATENCY(COMMIT3);
		} else {
			printf("Operation COMMIT3 is not supported\n");
		}
	} else if (!strcmp(params[0],"READ3")) {
		if (nb_ops.read3) {
			nb_ops.read3(child, fname, ival(params[2]),
				ival(params[3]), status);
			OP_LATENCY(READ3);
		} else {
			printf("Operation READ3 is not supported\n");
		}
	} else if (!strcmp(params[0],"ACCESS3")) {
		if (nb_ops.access3) {
			nb_ops.access3(child, fname, ival(params[2]),
				ival(params[3]), status);
			OP_LATENCY(ACCESS3);
		} else {
			printf("Operation ACCESS3 is not supported\n");
		}
	} else if (!strcmp(params[0],"MKDIR3")) {
		if (nb_ops.mkdir3) {
			nb_ops.mkdir3(child, fname, status);
			OP_LATENCY(MKDIR3);
		} else {
			printf("Operation MKDIR3 is not supported\n");
		}
	} else if (!strcmp(params[0],"RMDIR3")) {
		if (nb_ops.rmdir3) {
			nb_ops.rmdir3(child, fname, status);
			OP_LATENCY(RMDIR3);
		} else {
			printf("Operation RMDIR3 is not supported\n");
		}
	} else if (!strcmp(params[0],"FSSTAT3")) {
		if (nb_ops.fsstat3) {
			nb_ops.fsstat3(child, status);
			OP_LATENCY(FSSTAT3);
		} else {
			printf("Operation FSSTAT3 is not supported\n");
		}
	} else if (!strcmp(params[0],"FSINFO3")) {
		if (nb_ops.fsinfo3) {
			nb_ops.fsinfo3(child, status);
			OP_LATENCY(FSINFO3);
		} else {
			printf("Operation FSINFO3 is not supported\n");
		}
	} else if (!strcmp(params[0],"SYMLINK3")) {
		if (nb_ops.symlink3) {
			nb_ops.symlink3(child, fname, fname2, status);
			OP_LATENCY(SYMLINK3);
		} else {
			printf("Operation SYMLINK3 is not supported\n");
		}
	} else if (!strcmp(params[0],"LINK3")) {
		if (nb_ops.link3) {
			nb_ops.link3(child, fname, fname2, status);
			OP_LATENCY(LINK3);
		} else {
			printf("Operation LINK3 is not supported\n");
		}
	} else if (!strcmp(params[0],"REMOVE3")) {
		if (nb_ops.remove3) {
			nb_ops.remove3(child, fname, status);
			OP_LATENCY(REMOVE3);
		} else {
			printf("Operation REMOVE3 is not supported\n");
		}
	} else if (!strcmp(params[0],"READDIRPLUS3")) {
		if (nb_ops.readdirplus3) {
			nb_ops.readdirplus3(child, fname, status);
			OP_LATENCY(READDIRPLUS3);
		} else {
			printf("Operation READDIRPLUS3 is not supported\n");
		}
	} else if (!strcmp(params[0],"Sleep")) {
		nb_sleep(child, ival(params[1]), status);
	} else {
		printf("[%d] Unknown operation %s in pid %d\n", 
		       child->line, params[0], getpid());
	}
}


/* run a test that simulates an approximate netbench client load */
void child_run(struct child_struct *child0, const char *loadfile)
{
	int i;
	char line[1024], fname[1024], fname2[1024];
	char **sparams, **params;
	char *p;
	const char *status;
	FILE *f;
	pid_t parent = getppid();
	double targett;
	struct child_struct *child;

	for (child=child0;child<child0+options.clients_per_process;child++) {
		child->line = 0;
		asprintf(&child->cname,"client%d", child->id);
	}

	sparams = calloc(20, sizeof(char *));
	for (i=0;i<20;i++) {
		sparams[i] = malloc(100);
	}

	f = fopen(loadfile, "r");
	if (f == NULL) {
		perror(loadfile);
		exit(1);
	}

again:
	for (child=child0;child<child0+options.clients_per_process;child++) {
		nb_time_reset(child);
	}

	while (fgets(line, sizeof(line)-1, f)) {
		params = sparams;

		if (kill(parent, 0) == -1) {
			exit(1);
		}

		for (child=child0;child<child0+options.clients_per_process;child++) {
			if (child->done) goto done;
			child->line++;
		}

		line[strlen(line)-1] = 0;

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

		if (strncmp(params[i-1], "NT_STATUS_", 10) != 0 &&
		    strncmp(params[i-1], "0x", 2) != 0) {
			printf("Badly formed status at line %d\n", child->line);
			continue;
		}

		status = params[i-1];
		
		for (child=child0;child<child0+options.clients_per_process;child++) {
			fname[0] = 0;
			fname2[0] = 0;

			if (i>1 && params[1][0] == '/') {
				snprintf(fname, sizeof(fname), "%s%s", child->directory, params[1]);
				all_string_sub(fname,"client1", child->cname);
			}
			if (i>2 && params[2][0] == '/') {
				snprintf(fname2, sizeof(fname2), "%s%s", child->directory, params[2]);
				all_string_sub(fname2,"client1", child->cname);
			}

			if (options.targetrate != 0 || targett == 0.0) {
				nb_target_rate(child, options.targetrate);
			} else {
				nb_time_delay(child, targett);
			}
			child_op(child, params, fname, fname2, status);
		}
	}

	rewind(f);
	goto again;

done:
	fclose(f);
	for (child=child0;child<child0+options.clients_per_process;child++) {
		child->cleanup = 1;
		fflush(stdout);
		if (!options.skip_cleanup) {
			nb_ops.cleanup(child);
		}
		child->cleanup_finished = 1;
	}
}
