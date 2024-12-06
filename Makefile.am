
bin_PROGRAMS = dbench

dbench_SOURCES = fileio.c util.c dbench.c child.c system.c snprintf.c sockio.c nfsio.c blockio.c libnfs-glue.c socklib.c \
	linux_scsi.c libiscsi.c

LIBS += -lz

if HAVE_LIBNFS
LIBS += -lnfs
endif

if HAVE_LIBISCSI
LIBS += -liscsi
endif

