/* 
   dbench version 1.01
   
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

int sync_ops = 0;

static void sigcont(void)
{
}

/* this creates the specified number of child processes and runs fn()
   in all of them */
static double create_procs(int nprocs, void (*fn)(int ))
{
	int i, status;
	volatile int *child_status;
	int synccount;

	signal(SIGCONT, sigcont);

	start_timer();

	synccount = 0;

	if (nprocs < 1) {
		fprintf(stderr,
			"create %d procs?  you must be kidding.\n",
			nprocs);
		return 1;
	}

	child_status = (volatile int *)shm_setup(sizeof(int)*nprocs);
	if (!child_status) {
		printf("Failed to setup shared memory\n");
		return end_timer();
	}

	memset((void *)child_status, 0, sizeof(int)*nprocs);

	for (i=0;i<nprocs;i++) {
		if (fork() == 0) {
			setbuffer(stdout, NULL, 0);
			nb_setup(i);
			child_status[i] = getpid();
			pause();
			fn(i);
			_exit(0);
		}
	}

	do {
		synccount = 0;
		for (i=0;i<nprocs;i++) {
			if (child_status[i]) synccount++;
		}
		if (synccount == nprocs) break;
		sleep(1);
	} while (end_timer() < 30);

	if (synccount != nprocs) {
		printf("FAILED TO START %d CLIENTS (started %d)\n", nprocs, synccount);
		return end_timer();
	}

	start_timer();
	kill(0, SIGCONT);

	printf("%d clients started\n", nprocs);

	for (i=0;i<nprocs;i++) {
		waitpid(0, &status, 0);
		printf("*");
	}
	printf("\n");
	return end_timer();
}


static void show_usage(void)
{
	printf("usage: dbench [OPTIONS] nprocs\n"
	       "options:\n"
	       "  -c CLIENT.TXT    set location of client.txt\n"
	       "  -s               synchronous mode (like NFS2)\n");
	exit(1);
}



int process_opts(int argc, char **argv,
		 int *nprocs)
{
	int c;
	extern char *client_filename;
	extern int sync_ops;
	extern char *server;

	while ((c = getopt(argc, argv, "c:s")) != -1) 
		switch (c) {
		case 'c':
			client_filename = optarg;
			break;
		case 's':
			sync_ops = 1;
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
	double t;

	if (!process_opts(argc, argv, &nprocs))
		show_usage();

	t = create_procs(nprocs, child_run);

	/* to produce a netbench result we scale accoding to the
           netbench measured throughput for the run that produced the
           sniff that was used to produce client.txt. That run used 2
           clients and ran for 660 seconds to produce a result of
           4MBit/sec. */
	printf("Throughput %g MB/sec (NB=%g MB/sec  %g MBit/sec)%s\n", 
	       132*nprocs/t, 0.5*0.5*nprocs*660/t, 2*nprocs*660/t,
	       sync_ops ? " (sync mode)" : "");
	return 0;
}
