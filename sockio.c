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

#define MAX_FILES 1000

static char buf[70000];

extern int line_count;

char *server = "localhost";

static int sock;

/* emulate a single SMB packet exchange */
static void do_packets(unsigned long send_size, unsigned long recv_size)
{
	uint32 *ubuf = (uint32 *)buf;
	static int counter;

	ubuf[0] = htonl(send_size-4);
	ubuf[1] = htonl(recv_size-4);

	if (write_sock(sock, buf, send_size) != send_size) {
		printf("error writing %d bytes\n", (int)send_size);
		exit(1);
	}

	if (read_sock(sock, buf, recv_size) != recv_size) {
		printf("error reading %d bytes\n", (int)recv_size);
		exit(1);
	}

	if (ntohl(ubuf[0]) != recv_size-4) {
		printf("(%d) lost sync (%d %d)\n", 
		       line_count, (int)recv_size-4, (int)ntohl(ubuf[0]));
	}

	if (counter++ % 3000 == 0) printf(".");
}

void nb_setup(int client)
{
	extern char *tcp_options;

	sock = open_socket_out(server, TCP_PORT);
	if (sock == -1) {
		printf("client %d failed to start\n", client);
		exit(1);
	}
	set_socket_options(sock, tcp_options);

	do_packets(8, 8);
}


void nb_unlink(char *fname)
{
	do_packets(83, 39);
}

void nb_open(char *fname, int handle, int size)
{
	do_packets(111, 69);
}

void nb_write(int handle, int size, int offset)
{
	do_packets(51+size, 41);
}

void nb_read(int handle, int size, int offset)
{
	do_packets(55, size+4);
}

void nb_close(int handle)
{
	do_packets(45, 39);
}

void nb_mkdir(char *fname)
{
	do_packets(39+strlen(fname), 39);
}

void nb_rmdir(char *fname)
{
	do_packets(39+strlen(fname), 39);
}

void nb_rename(char *old, char *new)
{
	do_packets(41+strlen(old)+strlen(new), 39);
}


void nb_stat(char *fname, int size)
{
	do_packets(39+strlen(fname), 59);
}

void nb_create(char *fname, int size)
{
	nb_open(fname, 5000, size);
	nb_close(5000);
}
