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

#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/fcntl.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>

#ifndef MSG_WAITALL
#define MSG_WAITALL 0x100
#endif

#define MIN(x,y) ((x)<(y)?(x):(y))

#define TCP_PORT 7003
#define TCP_OPTIONS "TCP_NODELAY"

#define BOOL int
#define True 1
#define False 0
#define uint32 unsigned

#include "proto.h"

