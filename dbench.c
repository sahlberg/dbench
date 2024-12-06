/*
   Copyright (C) by Andrew Tridgell <tridge@samba.org> 1999-2007
   Copyright (C) 2001 by Martin Pool <mbp@samba.org>

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

/* TODO: We could try allowing for different flavours of synchronous
   operation: data sync and so on.  Linux apparently doesn't make any
   distinction, however, and for practical purposes it probably
   doesn't matter.  On NFSv4 it might be interesting, since the client
   can choose what kind it wants for each OPEN operation. */

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include "dbench.h"
#include <argp.h>
#include <zlib.h>

struct options options = {
	.timelimit           = 600,
	.directory           = ".",
	.tcp_options         = TCP_OPTIONS,
	.nprocs              = 10,
	.sync_open           = 0,
	.sync_dirs           = 0,
	.do_fsync            = 0,
	.fsync_frequency     = 0,
	.warmup              = -1,
	.targetrate          = 0.0,
	.ea_enable           = 0,
	.clients_per_process = 1,
	.server              = "localhost",
	.run_once            = 0,
	.allow_scsi_writes   = 0,
	.trunc_io            = 0,
#ifdef HAVE_LIBISCSI
	.iscsi_initiatorname = "iqn.2011-09.org.samba.dbench:client",
#endif
	.machine_readable    = 0,
};

static struct timeval tv_start;
static struct timeval tv_end;
static double throughput;
struct nb_operations *nb_ops;
int global_random;

static int check_loadfile(char *loadfile)
{
	gzFile f;

	if ((f = gzopen(loadfile, "rt")) == NULL) {
		fprintf(stderr, "dbench: error opening '%s': %s\n",
			loadfile, strerror(errno));
		return 0;
	}

	gzclose(f);
	return 1;
}


static struct child_struct *children;

static void sig_alarm(int sig)
{
	double total_bytes = 0;
	int total_lines = 0;
	int i;
	int nclients = options.nprocs * options.clients_per_process;
	int in_warmup = 0;
	double t;
	static int in_cleanup;
	double latency;
	struct timeval tnow;
	int num_active = 0;
	int num_finished = 0;
	(void)sig;

	tnow = timeval_current();

	for (i=0;i<nclients;i++) {
		total_bytes += children[i].bytes - children[i].bytes_done_warmup;
		if (children[i].bytes == 0 && options.warmup == -1) {
			in_warmup = 1;
		} else {
			num_active++;
		}
		total_lines += children[i].line;
		if (children[i].cleanup_finished) {
			num_finished++;
		}
	}

	t = timeval_elapsed(&tv_start);

	if (!in_warmup && options.warmup>0 && t > options.warmup) {
		tv_start = tnow;
		options.warmup = 0;
		for (i=0;i<nclients;i++) {
			children[i].bytes_done_warmup = children[i].bytes;
			children[i].worst_latency = 0;
			memset(&children[i].ops, 0, sizeof(children[i].ops));
		}
		goto next;
	}
	if (t < options.warmup) {
		in_warmup = 1;
	} else if (!in_warmup && !in_cleanup && t > options.timelimit) {
		for (i=0;i<nclients;i++) {
			children[i].done = 1;
		}
		tv_end = tnow;
		in_cleanup = 1;
	}
	if (t < 1) {
		goto next;
	}

	latency = 0;
	if (!in_cleanup) {
		for (i=0;i<nclients;i++) {
			latency = MAX(children[i].max_latency, latency);
			latency = MAX(latency, timeval_elapsed2(&children[i].lasttime, &tnow));
			children[i].max_latency = 0;
			if (latency > children[i].worst_latency) {
				children[i].worst_latency = latency;
			}
		}
	}

        if (in_warmup) {
		if (options.machine_readable) {
			printf("@W@%d@%d@%.2f@%u@%.03f@\n",
				num_active, total_lines/nclients,
				1.0e-6 * total_bytes / t, (int)t, latency*1000);
		} else {
			printf("%4d  %8d  %7.2f MB/sec  warmup %3.0f sec  latency %.03f ms\n",
				num_active, total_lines/nclients,
				1.0e-6 * total_bytes / t, t, latency*1000);
		}
        } else if (in_cleanup) {
		if (options.machine_readable) {
			printf("@C@%d@%d@%.2f@%u@%.03f@\n",
				num_active, total_lines/nclients,
				1.0e-6 * total_bytes / t, (int)t, latency*1000);
		} else {
			printf("%4d  cleanup %3.0f sec\n", nclients - num_finished, t);
		}
	} else {
		if (options.machine_readable) {
			printf("@R@%d@%d@%.2f@%u@%.03f@\n",
				num_active, total_lines/nclients,
				1.0e-6 * total_bytes / t, (int)t, latency*1000);
		} else {
			printf("%4d  %8d  %7.2f MB/sec  execute %3.0f sec  latency %.03f ms\n",
				nclients, total_lines/nclients,
				1.0e-6 * total_bytes / t, t, latency*1000);
				throughput = 1.0e-6 * total_bytes / t;
		}
	}

	fflush(stdout);
next:
	signal(SIGALRM, sig_alarm);
	alarm(PRINT_FREQ);
}


static void show_one_latency(struct op *ops, struct op *ops_all)
{
	int i;
	printf(" Operation                Count    AvgLat    MaxLat\n");
	printf(" --------------------------------------------------\n");
	for (i=0;nb_ops->ops[i].name;i++) {
		struct op *op1, *op_all;
		op1    = &ops[i];
		op_all = &ops_all[i];
		if (op_all->count == 0) continue;
		if (options.machine_readable) {
			printf(":%s:%u:%.03f:%.03f:\n",
				nb_ops->ops[i].name, op1->count,
				1000*op1->total_time/op1->count,
				op1->max_latency*1000);
		} else {
			printf(" %-22s %7u %9.03f %9.03f\n",
				nb_ops->ops[i].name, op1->count,
				1000*op1->total_time/op1->count,
				op1->max_latency*1000);
		}
	}
	printf("\n");
}

static void report_latencies(void)
{
	struct op sum[MAX_OPS];
	int i, j;
	struct op *op1, *op2;
	struct child_struct *child;

	memset(sum, 0, sizeof(sum));
	for (i=0;nb_ops->ops[i].name;i++) {
		op1 = &sum[i];
		for (j=0;j<options.nprocs * options.clients_per_process;j++) {
			child = &children[j];
			op2 = &child->ops[i];
			op1->count += op2->count;
			op1->total_time += op2->total_time;
			op1->max_latency = MAX(op1->max_latency, op2->max_latency);
		}
	}
	show_one_latency(sum, sum);

	if (!options.per_client_results) {
		return;
	}

	printf("Per client results:\n");
	for (i=0;i<options.nprocs * options.clients_per_process;i++) {
		child = &children[i];
		printf("Client %u did %u lines and %.0f bytes\n",
			i, child->line, child->bytes - child->bytes_done_warmup);
		show_one_latency(child->ops, sum);
	}
}

/* this creates the specified number of child processes and runs fn()
   in all of them */
static void create_procs(int nprocs, void (*fn)(struct child_struct *, const char *))
{
	int nclients = nprocs * options.clients_per_process;
	int i;
	pid_t *child_pids;

	for (i = 0; i < nclients; i++) {
		if (!check_loadfile(get_next_arg(options.loadfile, i))) {
			exit(1);
		}
	}

	if (nprocs < 1) {
		fprintf(stderr,
			"create %d procs?  you must be kidding.\n",
			nprocs);
		return;
	}

	children = shm_setup(sizeof(struct child_struct)*nclients);
	if (!children) {
		printf("Failed to setup shared memory\n");
		return;
	}

	memset(children, 0, sizeof(*children)*nclients);

	for (i = 0; i < nclients; i++) {
		children[i].id = i;
		children[i].num_clients = nclients;
		children[i].cleanup = 0;
		children[i].directory = options.directory;
		children[i].starttime = timeval_current();
		children[i].lasttime = timeval_current();
		children[i].all_children = children;
	}

	child_pids = malloc(sizeof(pid_t) * nprocs);
	memset(child_pids, 0, sizeof(pid_t) * nprocs);

	for (i = 0; i < nprocs; i++) {
		child_pids[i] = fork();
		if (child_pids[i] == 0) {
			int j;

			setlinebuf(stdout);
			srandom(getpid() ^ time(NULL));

			for (j=0;j<options.clients_per_process;j++) {
				nb_ops->setup(&children[i*options.clients_per_process + j]);
			}

			raise(SIGSTOP);

			fn(&children[i*options.clients_per_process],
			   get_next_arg(options.loadfile, i));
			_exit(0);
		}
	}

	printf("Waiting for child processes to finish setup.\n");
	for (i = 0; i < nprocs; i++) {
		int status = 0;

		do {
			waitpid(child_pids[i], &status, WUNTRACED);
			if (WIFSTOPPED(status)) {
				break;
			}
			if (WIFEXITED(status)) {
				printf("Child %d failed setup with status %d\n",
				       i, WEXITSTATUS(status));
				exit(1);
			}
			sleep(1);
		} while (1);
	}

	printf("Releasing clients\n");
	for (i = 0; i < nprocs; i++) {
		kill(child_pids[i], SIGCONT);
	}

	tv_start = timeval_current();

	signal(SIGALRM, sig_alarm);
	alarm(PRINT_FREQ);

	for (i = 0; i < nprocs;) {
		int status = 0;

		if (waitpid(0, &status, 0) == -1) {
			continue;
		}
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

	report_latencies();
}

static int parse_opt(int key, char *arg, struct argp_state *state)
{
	static unsigned int count = 0;

	switch (key) {
	case 'B':
		options.backend = arg;
		break;
	case 't':
		options.timelimit = atoi(arg);
		break;
	case 'c':
		options.loadfile = arg;
		break;
	case 'D':
		options.directory = arg;
		break;
	case 'T':
		options.tcp_options = arg;
		break;
	case 'R':
		options.targetrate = atof(arg);
		break;
	case 's':
		options.sync_open = 1;
		break;
	case 'S':
		options.sync_dirs = 1;
		break;
	case 'F':
		options.do_fsync = 1;
		break;
	case 'x':
		options.ea_enable = 1;
		break;
	case -1:
		options.no_resolve = 1;
		break;
	case -2:
		options.clients_per_process = atoi(arg);
		break;
	case -3:
		options.trunc_io = atoi(arg);
		break;
	case -4:
		options.one_byte_write_fix = 1;
		break;
	case -5:
		options.stat_check = 1;
		break;
	case -6:
		options.fake_io = 1;
		break;
	case -7:
		options.skip_cleanup = 1;
		break;
	case -8:
		options.per_client_results = 1;
		break;
	case -9:
		options.server = arg;
		break;
	case -10:
		options.nfs = arg;
		break;
	case -11:
		options.nlm = 1;
		break;
	case -12:
		options.run_once = 1;
		break;
	case -13:
		options.scsi_dev = arg;
		break;
	case -14:
		options.allow_scsi_writes = 1;
		break;
	case -15:
		options.iscsi_device = arg;
		break;
	case -16:
		options.iscsi_initiatorname = arg;
		break;
	case -17:
		options.warmup = atoi(arg);
		break;
	case -18:
		options.machine_readable = 1;
		break;
	case -19:
		options.smb_share = arg;
		break;
	case -20:
		options.smb_user = arg;
		break;
	case -21:
		options.block = arg;
		break;
	case ARGP_KEY_NO_ARGS:
		printf("You need to specify NPROCS\n");
		argp_usage(state);
		break;
	case ARGP_KEY_ARG:
		count++;
		if (count == 1)
			options.nprocs = atoi(arg);
		break;
	case ARGP_KEY_END:
		if (count > 1) {
			printf("too many arguments\n");
			argp_usage(state);
		}
		break;
	default:
		return ARGP_ERR_UNKNOWN;
	}
	return 0;
}

static void process_opts(int argc, char **argv)
{
	struct argp_option options[] =
	{
		{"backend", 'B', "STRING", 0, "dbench backend (fileio, sockio, nfs, scsi, iscsi, smb)", 0},
		{"timelimit", 't', "INTEGER", 0, "timelimit", 0},
		{"loadfile", 'c', "FILENAME", 0, "loadfile", 0},
		{"directory", 'D', "STRING", 0, "working directory", 0},
		{"tcp-options", 'T', "STRING", 0, "TCP socket options", 0},
		{"target-rate", 'R', "DOUBLE", 0, "target throughput (MB/sec)", 0},
		{"sync", 's', 0, 0, "use O_SYNC", 1},
		{"sync-dir", 'S', 0, 0, "sync directory changes", 1},
		{"fsync", 'F', 0, 0, "fsync on write", 1},
		{"xattr", 'x', 0, 0, "use xattrs", 1},
		{"no-resolve", -1, 0, 0, "disable name resolution simulation", 3},
		{"clients-per-process", -2, "INTEGER", 0, "number of clients per process", 2},
		{"trunc-io", -3, "INTEGER", 0, "truncate all io to this size", 2},
		{"one-byte-write-fix", -4, 0, 0, "try to fix 1 byte writes", 3},
		{"stat-check", -5, 0, 0, "check for pointless calls with stat", 3},
		{"fake-io", -6, 0, 0, "fake up read/write calls", 3},
		{"skip-cleanup", -7, 0, 0, "skip cleanup operations", 3},
		{"per-client-results", -8, 0, 0, "show results per client", 3},
		{"server", -9, "STRING", 0, "server", 2},
		{"nfs", -10, "STRING", 0, "nfs url", 2},
		{"enable-nlm", -11, 0, 0, "Enable support for NFS byte range locking commands", 3},
		{"run-once", -12, 0, 0, "Stop once reaching the end of the loadfile", 3},
		{"scsi", -13, "STRING", 0, "scsi device", 2},
		{"allow-scsi-writes", -14, 0, 0, "Allow SCSI write command to the device", 3},
#ifdef HAVE_LIBISCSI
		{"iscsi", -15, "STRING", 0, "iscsi URL for the target device", 2},
		{"iscsi-initiatorname", -16, "STRING", 0, "iscsi InitiatorName", 2},
#endif
		{"warmup", -17, "INTEGER", 0, "How many seconds of warmup to run", 2},
		{"machine-readable", -18, 0, 0, "Print data in more machine-readable friendly format", 3},
#ifdef HAVE_LIBSMBCLIENT
		{"smb-share", -19, "STRING", 0, "//SERVER/SHARE to use", 2},
		{"smb-user", -20, "STRING", 0, "User to authenticate as : [<domain>/]<user>%<password>", 2},
#endif
		{"block", -21, "STRING", 0, "Block device", 2},
		{ 0 }
	};

	struct argp argp = {
		.options = options,
		.parser = parse_opt
	};
	argp_parse(&argp, argc, argv, 0, 0, 0);

#ifndef HAVE_EA_SUPPORT
	if (options.ea_enable) {
		printf("EA suppport not compiled in\n");
		exit(1);
	}
#endif
}



 int main(int argc, char *argv[])
{
	double total_bytes = 0;
	double latency=0;
	int i;

	setlinebuf(stdout);

	printf("dbench version %s - Copyright Andrew Tridgell 1999-2004\n\n", VERSION);

	srandom(getpid() ^ time(NULL));
	global_random = random();

	process_opts(argc, argv);

	if (options.backend == NULL) {
		printf("No backend was specified. Aborting.\n");
		exit(10);
	}

	if (options.loadfile == NULL) {
		printf("No loadfile was specified. Aborting.\n");
		exit(10);
	}

	if (strcmp(options.backend, "fileio") == 0) {
		extern struct nb_operations fileio_ops;
		nb_ops = &fileio_ops;
	} else if (strcmp(options.backend, "sockio") == 0) {
		extern struct nb_operations sockio_ops;
		nb_ops = &sockio_ops;
	} else if (strcmp(options.backend, "block") == 0) {
		extern struct nb_operations block_ops;
		nb_ops = &block_ops;
#ifdef HAVE_LIBNFS
	} else if (strcmp(options.backend, "nfs") == 0) {
		extern struct nb_operations nfs_ops;
		nb_ops = &nfs_ops;
#endif
#ifdef HAVE_LINUX_SCSI_SG
	} else if (strcmp(options.backend, "scsi") == 0) {
		extern struct nb_operations scsi_ops;
		nb_ops = &scsi_ops;
#endif /* HAVE_LINUX_SCSI_SG */
#ifdef HAVE_LIBISCSI
	} else if (strcmp(options.backend, "iscsi") == 0) {
		extern struct nb_operations iscsi_ops;
		nb_ops = &iscsi_ops;
#endif
#ifdef HAVE_LIBSMBCLIENT
	} else if (strcmp(options.backend, "smb") == 0) {
		extern struct nb_operations smb_ops;
		nb_ops = &smb_ops;
#endif
	} else {
		printf("Unknown backend '%s'\n", options.backend);
		exit(1);
	}

	if (options.warmup == -1) {
		options.warmup = options.timelimit / 5;
	}

	if (nb_ops->init) {
		if (nb_ops->init() != 0) {
			printf("Failed to initialize dbench\n");
			exit(10);
		}
	}

	printf("Running for %d seconds with load '%s' and minimum warmup %d secs\n",
		options.timelimit, options.loadfile, options.warmup);

	create_procs(options.nprocs, child_run);

	for (i=0;i<options.nprocs*options.clients_per_process;i++) {
		total_bytes += children[i].bytes - children[i].bytes_done_warmup;
		latency = MAX(latency, children[i].worst_latency);
	}

	if (options.machine_readable) {
		printf(";%g;%d;%d;%.03f;\n",
			throughput,
			options.nprocs*options.clients_per_process,
			options.nprocs, latency*1000);
	} else {
		printf("Throughput %g MB/sec%s%s  %d clients  %d procs  max_latency=%.03f ms\n",
			throughput,
			options.sync_open ? " (sync open)" : "",
			options.sync_dirs ? " (sync dirs)" : "",
			options.nprocs*options.clients_per_process,
			options.nprocs, latency*1000);
	}
	return 0;
}
