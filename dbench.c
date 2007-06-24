/* 
   Copyright (C) by Andrew Tridgell <tridge@samba.org> 1999, 2001
   Copyright (C) 2001 by Martin Pool <mbp@samba.org>
   
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

/* TODO: We could try allowing for different flavours of synchronous
   operation: data sync and so on.  Linux apparently doesn't make any
   distinction, however, and for practical purposes it probably
   doesn't matter.  On NFSv4 it might be interesting, since the client
   can choose what kind it wants for each OPEN operation. */

#include "dbench.h"
#include <sys/sem.h>

int sync_open = 0, do_fsync = 0, sync_dirs = 0;
char *tcp_options = TCP_OPTIONS;
static int timelimit = 600, warmup;
static const char *directory = ".";
static char *loadfile = DATADIR "/client.txt";
static struct timeval tv_start;
static struct timeval tv_end;
static int barrier=-1;
double targetrate;

#if HAVE_EA_SUPPORT
int ea_enable=0;
#endif

static FILE *open_loadfile(void)
{
	FILE		*f;

	if ((f = fopen(loadfile, "rt")) != NULL)
		return f;

	fprintf(stderr,
		"dbench: error opening '%s': %s\n", loadfile,
		strerror(errno));

	return NULL;
}


static struct child_struct *children;

static void sem_cleanup() {
	if (!(barrier==-1)) 
		semctl(barrier,0,IPC_RMID);
}

static void sig_alarm(int sig)
{
	double total_bytes = 0;
	int total_lines = 0;
	int i;
	int nprocs = children[0].nprocs;
	int in_warmup = 0;
	double t;
	static int in_cleanup;
	double latency;
	struct timeval tnow;

	(void)sig;

	tnow = timeval_current();

	for (i=0;i<nprocs;i++) {
		total_bytes += children[i].bytes - children[i].bytes_done_warmup;
		if (children[i].bytes == 0) {
			in_warmup = 1;
		}
		total_lines += children[i].line;
	}

	t = timeval_elapsed(&tv_start);

	if (!in_warmup && warmup>0 && t > warmup) {
		tv_start = tnow;
		warmup = 0;
		for (i=0;i<nprocs;i++) {
			children[i].bytes_done_warmup = children[i].bytes;
		}
		goto next;
	}
	if (t < warmup) {
		in_warmup = 1;
	} else if (!in_warmup && !in_cleanup && t > timelimit) {
		for (i=0;i<nprocs;i++) {
			children[i].done = 1;
		}
		tv_end = tnow;
		in_cleanup = 1;
	}
	if (t < 1) {
		goto next;
	}

	latency = 0;
	for (i=0;i<nprocs;i++) {
		latency = MAX(children[i].max_latency, latency);
		latency = MAX(latency, timeval_elapsed2(&children[i].lasttime, &tnow));
		children[i].max_latency = 0;
	}

        if (in_warmup) {
                printf("%4d  %8d  %7.2f MB/sec  warmup %3.0f sec  latency %.03f ms \n", 
                       nprocs, total_lines/nprocs, 
                       1.0e-6 * total_bytes / t, t, latency*1000);
        } else if (in_cleanup) {
                printf("%4d  %8d  %7.2f MB/sec  cleanup %3.0f sec  latency %.03f ms\n", 
                       nprocs, total_lines/nprocs, 
                       1.0e-6 * total_bytes / t, t, latency*1000);
        } else {
                printf("%4d  %8d  %7.2f MB/sec  execute %3.0f sec  latency %.03f ms\n", 
                       nprocs, total_lines/nprocs, 
                       1.0e-6 * total_bytes / t, t, latency*1000);
        }

	fflush(stdout);
next:
	signal(SIGALRM, sig_alarm);
	alarm(PRINT_FREQ);
}

/* this creates the specified number of child processes and runs fn()
   in all of them */
static void create_procs(int nprocs, void (*fn)(struct child_struct *, const char *))
{
	int i, status;
	int synccount;
	struct timeval tv;
	FILE *load;
	struct sembuf sbuf;
	double t;

	load = open_loadfile();
	if (load == NULL) {
		exit(1);
	}

	if (nprocs < 1) {
		fprintf(stderr,
			"create %d procs?  you must be kidding.\n",
			nprocs);
		return;
	}

	children = shm_setup(sizeof(struct child_struct)*nprocs);
	if (!children) {
		printf("Failed to setup shared memory\n");
		return;
	}

	memset(children, 0, sizeof(*children)*nprocs);

	for (i=0;i<nprocs;i++) {
		children[i].id = i;
		children[i].nprocs = nprocs;
		children[i].cleanup = 0;
		children[i].directory = directory;
		children[i].starttime = timeval_current();
	}

	if (atexit(sem_cleanup) != 0) {
		printf("can't register cleanup function on exit\n");
		exit(1);
	}
	sbuf.sem_num =  0;
	if ( !(barrier = semget(IPC_PRIVATE,1,IPC_CREAT | S_IRUSR | S_IWUSR)) ) {
		printf("failed to create barrier semaphore \n");
	}
	sbuf.sem_flg =  SEM_UNDO;
	sbuf.sem_op  =  1;
	if (semop(barrier, &sbuf, 1) == -1) {
		printf("failed to initialize the barrier semaphore\n");
		exit(1);
	}
	sbuf.sem_flg =  0;

	for (i=0;i<nprocs;i++) {
		if (fork() == 0) {
			setlinebuf(stdout);
			nb_setup(&children[i]);

			sbuf.sem_op  =  0;
			if (semop(barrier, &sbuf, 1) == -1) {
				printf("failed to use the barrier semaphore in child %d\n",getpid());
				exit(1);
			}

			semctl(barrier,0,IPC_RMID);

			fn(&children[i], loadfile);
			_exit(0);
		}
	}

	synccount = 0;
	tv = timeval_current();
	do {
		synccount = semctl(barrier,0,GETZCNT);
		t = timeval_elapsed(&tv);
		printf("%d of %d clients prepared for launch %3.0f sec\n", synccount, nprocs, t);
		if (synccount == nprocs) break;
		usleep(100*1000);
	} while (timeval_elapsed(&tv) < 30);

	if (synccount != nprocs) {
		printf("FAILED TO START %d CLIENTS (started %d)\n", nprocs, synccount);
		return;
	}

	printf("releasing clients\n");
	tv_start = timeval_current();
	sbuf.sem_op  =  -1;
	if (semop(barrier, &sbuf, 1) == -1) {
		printf("failed to release barrier\n");
		exit(1);
	}

	semctl(barrier,0,IPC_RMID);

	signal(SIGALRM, sig_alarm);
	alarm(PRINT_FREQ);

	for (i=0;i<nprocs;) {
		if (waitpid(0, &status, 0) == -1) continue;
		if (WEXITSTATUS(status) != 0) {
			printf("Child failed with status %d\n",
			       WEXITSTATUS(status));
			exit(1);
		}
		i++;
	}

	alarm(0);
	sig_alarm(SIGALRM);

	printf("\n");
}


static void show_usage(void)
{
	printf("usage: dbench [OPTIONS] nprocs\n" \
	       "usage: tbench [OPTIONS] nprocs <server>\n" \
	       "options:\n" \
	       "  -v               show version\n" \
	       "  -t timelimit     run time in seconds (default 600)\n" \
	       "  -D directory     base directory to run in\n" \
	       "  -c loadfile      set location of the loadfile\n" \
	       "  -R               target rate (MByte/sec)\n" \
	       "  -s               synchronous file IO\n" \
	       "  -F               fsync on write\n" \
	       "  -S               synchronous directories (mkdir, unlink...)\n" \
	       "  -x               enable EA support\n" \
	       "  -T options       set socket options for tbench\n");
	exit(1);
}



static int process_opts(int argc, char **argv,
			int *nprocs)
{
	int c;
	extern int sync_open;
	extern char *server;
	
	targetrate = 0;

	while ((c = getopt(argc, argv, "vc:sST:t:xD:R:F")) != -1) 
		switch (c) {
		case 'c':
			loadfile = optarg;
			break;
		case 's':
			sync_open = 1;
			break;
		case 'F':
			do_fsync = 1;
			break;
		case 'S':
			sync_dirs = 1;
			break;
		case 'T':
			tcp_options = optarg;
			break;
		case 't':
			timelimit = atoi(optarg);
			break;
		case 'D':
			directory = optarg;
			break;
		case 'v':
			exit(0);
			break;
		case 'R':
			targetrate = strtod(optarg, NULL);
			break;
		case 'x':

#if HAVE_EA_SUPPORT
			ea_enable = 1;
#else
			printf("EA suppport not compiled in\n");
			exit(1);
#endif
			break;
		case '?':
			if (isprint (optopt))
				fprintf (stderr, "Unknown option `-%c'.\n", optopt);
			else
				fprintf (stderr,
					 "Unknown option character `\\x%x'.\n",
					 optopt);
			return 0;
		default:
			abort ();
		}
	
	if (!argv[optind])
		return 0;
	
	*nprocs = atoi(argv[optind++]);

	if (argv[optind])
		server = argv[optind++];

	return 1;
}



 int main(int argc, char *argv[])
{
	int nprocs;
	double total_bytes = 0;
	double t;
	int i;

	printf("dbench version %s - Copyright Andrew Tridgell 1999-2004\n\n", VERSION);

	if (!process_opts(argc, argv, &nprocs))
		show_usage();

	warmup = timelimit / 5;

        printf("Running for %d seconds with load '%s' and minimum warmup %d secs\n", 
               timelimit, loadfile, warmup);

	create_procs(nprocs, child_run);

	for (i=0;i<nprocs;i++) {
		total_bytes += children[i].bytes - children[i].bytes_done_warmup;
	}

	t = timeval_elapsed2(&tv_start, &tv_end);

	printf("Throughput %g MB/sec%s%s %d procs\n", 
	       1.0e-6 * total_bytes / t,
	       sync_open ? " (sync open)" : "",
	       sync_dirs ? " (sync dirs)" : "", nprocs);
	return 0;
}
