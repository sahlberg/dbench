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

#include "dbench.h"

static void sigusr1(void)
{
}

/* this creates the specified number of child processes and runs fn() in all of them */
static double create_procs(int nprocs, void (*fn)(int ))
{
	int i, status;
	volatile int *child_status;
	int synccount;

	signal(SIGUSR1, sigusr1);

	start_timer();

	synccount = 0;

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
	kill(0, SIGUSR1);

	printf("%d clients started\n", nprocs);

	for (i=0;i<nprocs;i++) {
		waitpid(0, &status, 0);
		printf("*");
	}
	printf("\n");
	return end_timer();
}




 int main(int argc, char *argv[])
{
	extern char *server;
	int nprocs;
	double t;

	if (argc < 2) {
		printf("usage: dbench nprocs\n");
		exit(1);
	}

	nprocs = atoi(argv[1]);

	if (argc > 2) {
		server = argv[2];
	}

	t = create_procs(nprocs, child_run);

	/* to produce a netbench result we scale accoding to the
           netbench measured throughput for the run that produced the
           sniff that was used to produce client.txt. That run used 2
           clients and ran for 660 seconds to produce a result of
           4MBit/sec. */
	printf("Throughput %g MB/sec (NB=%g MB/sec  %g MBit/sec)\n", 
	       132*nprocs/t, 0.5*0.5*nprocs*660/t, 2*nprocs*660/t);
	return 0;
}
