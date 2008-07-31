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

static void nb_sleep(int usec)
{
	usleep(usec);
}


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
static void child_op(struct child_struct *child, const char *opname,
		     const char *fname, const char *fname2, 
		     char **params, const char *status)
{
	struct dbench_op op;
	unsigned i;

	child->lasttime = timeval_current();

	ZERO_STRUCT(op);
	op.child = child;
	op.op = opname;
	op.fname = fname;
	op.fname2 = fname2;
	op.status = status;
	for (i=0;i<sizeof(op.params)/sizeof(op.params[0]);i++) {
		op.params[i] = params[i]?ival(params[i]):0;
	}

	if (strcasecmp(op.op, "Sleep") == 0) {
		nb_sleep(op.params[0]);
		return;
	}

	for (i=0;nb_ops->ops[i].name;i++) {
		if (strcasecmp(op.op, nb_ops->ops[i].name) == 0) {
			nb_ops->ops[i].fn(&op);
			finish_op(child, &child->ops[i]);
			return;
		}
	}

	printf("[%u] Unknown operation %s in pid %u\n", 
	       child->line, op.op, (unsigned)getpid());
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
			int pcount = 1;

			fname[0] = 0;
			fname2[0] = 0;

			if (i>1 && params[1][0] == '/') {
				snprintf(fname, sizeof(fname), "%s%s", child->directory, params[1]);
				all_string_sub(fname,"client1", child->cname);
				pcount++;
			}
			if (i>2 && params[2][0] == '/') {
				snprintf(fname2, sizeof(fname2), "%s%s", child->directory, params[2]);
				all_string_sub(fname2,"client1", child->cname);
				pcount++;
			}

			if (options.targetrate != 0 || targett == 0.0) {
				nb_target_rate(child, options.targetrate);
			} else {
				nb_time_delay(child, targett);
			}
			child_op(child, params[0], fname, fname2, params+pcount, status);
		}
	}

	if (options.run_once) {
		goto done;
	}

	rewind(f);
	goto again;

done:
	fclose(f);
	for (child=child0;child<child0+options.clients_per_process;child++) {
		child->cleanup = 1;
		fflush(stdout);
		if (!options.skip_cleanup) {
			nb_ops->cleanup(child);
		}
		child->cleanup_finished = 1;
	}
}
