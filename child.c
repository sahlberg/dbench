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
#include <zlib.h>

#define ival(s) strtoll(s, NULL, 0)

#define RWBUFSIZE 16*1024*1024
char rw_buf[RWBUFSIZE];

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

/* here we parse "special" arguments that start with '*'
 * '*' itself means a random 64 bit number, but this can be qualified as
 *
 * '*'     a random 64 bit number
 * '...%y' modulo y
 * '.../y' align the number as an integer multiple of y  (( x = (x/y)*y))
 * '...+y' add 'y'
 *
 * Examples :
 * '*'       : random 64 bit number
 * '*%1024'  : random number between 0 and 1023
 * '* /1024'  : random 64 bit number aligned to n*1024
 * '*%1024/2 : random even number between 0 and 1023
 *
 *
 * a special case is when the format starts with a '+' and is followed by
 * a number, in which case we reuse the number from the previous line in the
 * loadfile and add <number> to it :
 * '+1024' : add 1024 to the value from the previous line in the loadfile
 */
static uint64_t parse_special(const char *fmt, uint64_t prev_val)
{
	char q;
	uint64_t num;
	uint64_t val;

	if (*fmt == '+') {
		val = strtoll(fmt+1, NULL, 0);
		return prev_val + val;
	}

	num = random();
	num = (num <<32) | random();

	fmt++;
	while (*fmt != '\0') {
		q = *fmt++;
		val = strtoll(fmt, NULL, 0);
		if (val == 0) {
			printf("Illegal value in random number qualifier. Can not be zero\n");
			return num;
		}

		switch (q) {
		case '/':
			num = (num/val)*val;
			break;
		case '%':
			num = num%val;
			break;
		case '+':
			num = num+val;
			break;
		default:
			printf("Unknown qualifier '%c' for randum number qualifier\n", q);
		}

		/* skip until the next token */
		while (*fmt != '\0') {
			switch (*fmt) {
			case '0'...'9':
			case 'a'...'f':
			case 'A'...'F':
			case 'x':
			case 'X':
				fmt++;
				continue;
			}
			break;
		}
	}

	return num;
}
/*
  one child operation
 */
static void child_op(struct child_struct *child, const char *opname,
		     const char *fname, const char *fname2, 
		     char **params, const char *status)
{
	static struct dbench_op prev_op;
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
		switch (params[i][0]) {
		case '*':
		case '+':
			op.params[i] = parse_special(params[i], prev_op.params[i]);
			break;
		default:
			op.params[i] = params[i]?ival(params[i]):0;
		}
	}

	prev_op = op;

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

#define MAX_RND_STR 10
static char random_string[MAX_RND_STR][256];

static int store_random_string(unsigned int idx, char *str)
{
	if (idx >= MAX_RND_STR) {
		fprintf(stderr, "'idx' in RANDOMSTRING is too large. %u specified but %u is maximum\n", idx, MAX_RND_STR-1);
		return 1;
	}


	strncpy(random_string[idx], str, sizeof(random_string[0]));

	return 0;
}

static char *get_random_string(unsigned int idx)
{
	return random_string[idx];
}

/*
 * This parses a line of the form :
 * RANDOMSTRING <idx> <string>
 * All subpatterns of the form [<characters>] in string are substituted for
 * a randomly chosen character from the specified set.
 *
 * The end result is stored as string index <idx>
 */
static int parse_randomstring(char *line)
{
	int num;
	char *pstart, *pend, rndc[2];
	unsigned int idx;
	char str[256];

again:
	pstart = index(line, '[');
	if (pstart == NULL) {
		goto finished;
	}
	strncpy(str, pstart, sizeof(str));

	pend = index(str, ']');
	if (pstart == NULL) {
		fprintf(stderr, "Unbalanced '[' in RANDOMSTRING : %s\n", line);
		return 1;
	}

	pend++;
	*pend = '\0';

	/* pick a random character */
	num = strlen(str) - 2;
	rndc[0] = str[random()%num + 1];
	rndc[1] = '\0';

	single_string_sub(line, str, rndc);
	goto again;


finished:
	if (sscanf(line, "RANDOMSTRING %u %s\n", &idx, str) != 2) {
		fprintf(stderr, "Invalid RANDOMSTRING line : [%s]\n", line);
		return 1;
	}
	/* remote initial " */
	while (str[0] == '"') {
		memcpy(str, str+1, sizeof(str)-1);
	}
	/* remote trailing " */
	while (1) {
		int len = strlen(str);

		if (len < 1) {
			break;
		}

		if (str[len-1] != '"') {
			break;
		}

		str[len-1] = '\0';
	}

	if (store_random_string(idx, str)) {
		fprintf(stderr, "Failed to store randomstring idx:%d str:%s\n", idx, str);
		return 1;
	}

	return 0;
}


/* run a test that simulates an approximate netbench client load */
#define MAX_PARM_LEN 1024
void child_run(struct child_struct *child0, const char *loadfile)
{
	int i;
	char line[MAX_PARM_LEN], fname[MAX_PARM_LEN], fname2[MAX_PARM_LEN];
	char **sparams, **params;
	char *p;
	const char *status;
	gzFile *gzf;
	pid_t parent = getppid();
	double targett;
	struct child_struct *child;
	int have_random = 0;
	unsigned loop_count = 0;
	z_off_t loop_start = 0;

	gzf = gzopen(loadfile, "r");
	if (gzf == NULL) {
		perror(loadfile);
		exit(1);
	}

	for (child=child0;child<child0+options.clients_per_process;child++) {
		child->line = 0;
		asprintf(&child->cname,"client%d", child->id);
	}

	sparams = calloc(20, sizeof(char *));
	for (i=0;i<20;i++) {
		sparams[i] = malloc(MAX_PARM_LEN);
		memset(sparams[i], 0, MAX_PARM_LEN);
	}

again:
	for (child=child0;child<child0+options.clients_per_process;child++) {
		nb_time_reset(child);
	}

	while (gzgets(gzf, line, sizeof(line)-1)) {
		unsigned repeat_count = 1;

		for (child=child0;child<child0+options.clients_per_process;child++) {
			if (child->done) goto done;
			child->line++;
		}


		params = sparams;

		if (kill(parent, 0) == -1) {
			exit(1);
		}

loop_again:
		/* if this is a "LOOP <xxx>" line, 
		 * remember the current file position and move to the next line
		 */
		if (strncmp(line, "LOOP", 4) == 0) {
			if (sscanf(line, "LOOP %u\n", &loop_count) != 1) {
				fprintf(stderr, "Incorrect LOOP at line %d\n", child0->line);
				goto done;
			}

	       		for (child=child0;child<child0+options.clients_per_process;child++) {
				child->line++;
			}
			loop_start = gztell(gzf);
			gzgets(gzf, line, sizeof(line)-1);
			goto loop_again;
	        }

		if (strncmp(line, "ENDLOOP", 7) == 0) {
			loop_count--;

			gzgets(gzf, line, sizeof(line)-1);

			if (loop_count > 0) {
				gzseek(gzf, loop_start, SEEK_SET);
			}
			
			gzgets(gzf, line, sizeof(line)-1);
			goto loop_again;
		}			

		/* if this is a "REPEAT <xxx>" line, just replace the
		 * currently read line with the next line
		 */
		if (strncmp(line, "REPEAT", 6) == 0) {
			if (sscanf(line, "REPEAT %u\n", &repeat_count) != 1) {
				fprintf(stderr, "Incorrect REPEAT at line %d\n", child0->line);
				goto done;
			}

	       		for (child=child0;child<child0+options.clients_per_process;child++) {
				child->line++;
			}
			gzgets(gzf, line, sizeof(line)-1);
	        }


		/* WRITEPATTERN */
		if (strncmp(line, "WRITEPATTERN", 12) == 0) {
			char *ptr = rw_buf;
			int count = RWBUFSIZE;
			
			while (count > 0) {
			      int len;

			      len = count;
			      if (len > strlen(line +13)) {
			     	      len = strlen(line +13);
			      }
			      memcpy(ptr, line+13, len);
			      ptr += len;
			      count -= len;
			}
			goto again;
		}


		/* RANDOMSTRING */
		if (strncmp(line, "RANDOMSTRING", 12) == 0) {
			have_random = 1;
			if (parse_randomstring(line) != 0) {
				fprintf(stderr, "Incorrect RANDOMSTRING at line %d\n", child0->line);
				goto done;
			}
			goto again;
		}


		line[strlen(line)-1] = 0;

		all_string_sub(line,"\\", "/");
		all_string_sub(line," /", " ");

		/* substitute all $<digit> stored strings */
		while (have_random && (p = index(line, '$')) != NULL) {
			char sstr[3], *nstr;
			unsigned int idx;
		      
		      	idx = *(p+1) - '0';
			if (idx >= MAX_RND_STR) {
				fprintf(stderr, "$%d is an invalid filename/string\n", idx);
				goto done;
			}

			sstr[0] = '$';
			sstr[1] = idx+'0';
			sstr[2] = '\0';

			nstr = get_random_string(idx);
			all_string_sub(line, sstr, nstr);
		}
		
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
		    strncmp(params[i-1], "0x", 2) != 0 &&
		    strncmp(params[i-1], "SUCCESS", 7) != 0 &&
		    strncmp(params[i-1], "ERROR", 7) != 0 &&
		    strncmp(params[i-1], "*", 1) != 0) {
			printf("Badly formed status at line %d\n", child->line);
			continue;
		}

		status = params[i-1];

		
		for (child=child0;child<child0+options.clients_per_process;child++) {
			unsigned child_repeat_count = repeat_count;
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
			while (child_repeat_count--) {
				child_op(child, params[0], fname, fname2, params+pcount, status);
			}
		}
	}

	if (options.run_once) {
		goto done;
	}

	gzrewind(gzf);
	goto again;

done:
	gzclose(gzf);
	for (child=child0;child<child0+options.clients_per_process;child++) {
		child->cleanup = 1;
		fflush(stdout);
		if (!options.skip_cleanup) {
			nb_ops->cleanup(child);
		}
		child->cleanup_finished = 1;
		if(child->cname){
			free(child->cname);
			child->cname = NULL;
		}
	}
}
