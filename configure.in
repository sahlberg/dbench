dnl Process this file with autoconf to produce a configure script.

AC_INIT()
AC_PREREQ(2.52)

AC_MSG_NOTICE([Configuring dbench])

AC_CONFIG_HEADER(config.h)

dnl Checks for programs.
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL

AC_DEFINE([_GNU_SOURCE], 1,
          [Define _GNU_SOURCE so that we get all necessary prototypes])

# If GCC, turn on warnings.
if test "x$GCC" = "xyes"
then
	CFLAGS="$CFLAGS -Wall -W"
else
	CFLAGS="$CFLAGS -O"
fi

AC_HEADER_DIRENT
AC_HEADER_TIME
AC_HEADER_SYS_WAIT

AC_CHECK_HEADERS(ctype.h strings.h stdlib.h string.h sys/vfs.h sys/statvfs.h stdint.h)

AC_CHECK_HEADERS(sys/attributes.h attr/xattr.h sys/xattr.h sys/extattr.h sys/uio.h)
AC_CHECK_HEADERS(sys/mount.h)

AC_CHECK_FUNCS(fdatasync)
# Check if we have libattr
AC_SEARCH_LIBS(getxattr, [attr])
AC_SEARCH_LIBS(socket, [socket])
AC_SEARCH_LIBS(gethostbyname, [nsl])

AC_CHECK_FUNCS(getxattr lgetxattr fgetxattr listxattr llistxattr)
AC_CHECK_FUNCS(flistxattr removexattr lremovexattr fremovexattr)
AC_CHECK_FUNCS(setxattr lsetxattr fsetxattr)
# Check if we have attr_get
AC_CHECK_FUNCS(attr_get attr_list attr_set attr_remove)
AC_CHECK_FUNCS(attr_getf attr_listf attr_setf attr_removef)
# Check if we have extattr
AC_CHECK_FUNCS(extattr_delete_fd extattr_delete_file extattr_delete_link)
AC_CHECK_FUNCS(extattr_get_fd extattr_get_file extattr_get_link)
AC_CHECK_FUNCS(extattr_list_fd extattr_list_file extattr_list_link)
AC_CHECK_FUNCS(extattr_set_fd extattr_set_file extattr_set_link)
AC_CHECK_FUNCS(snprintf vsnprintf asprintf vasprintf)

if test x"$ac_cv_func_fgetxattr" = x"yes" -o \
        x"$ac_cv_func_attr_getf" = x"yes" -o \
        x"$ac_cv_func_extattr_get_fd" = x"yes"; then
	    AC_DEFINE(HAVE_EA_SUPPORT, 1, [Whether we have EA support])
fi

AC_CACHE_CHECK([for va_copy],dbench_cv_HAVE_VA_COPY,[
AC_TRY_LINK([#include <stdarg.h>
va_list ap1,ap2;], [va_copy(ap1,ap2);],
dbench_cv_HAVE_VA_COPY=yes,dbench_cv_HAVE_VA_COPY=no)])
if test x"$dbench_cv_HAVE_VA_COPY" = x"yes"; then
    AC_DEFINE(HAVE_VA_COPY,1,[Whether va_copy() is available])
fi

if test x"$dbench_cv_HAVE_VA_COPY" != x"yes"; then
AC_CACHE_CHECK([for __va_copy],dbench_cv_HAVE___VA_COPY,[
AC_TRY_LINK([#include <stdarg.h>
va_list ap1,ap2;], [__va_copy(ap1,ap2);],
dbench_cv_HAVE___VA_COPY=yes,dbench_cv_HAVE___VA_COPY=no)])
if test x"$dbench_cv_HAVE___VA_COPY" = x"yes"; then
    AC_DEFINE(HAVE___VA_COPY,1,[Whether __va_copy() is available])
fi
fi

#
# Check that POPT is available
#
AC_MSG_CHECKING(whether libpopt is available)
ac_save_CFLAGS="$CFLAGS"
ac_save_LIBS="$LIBS"
CFLAGS="$CFLAGS $GLIB_CFLAGS"
LIBS="$GLIB_LIBS $LIBS -lpopt"
AC_TRY_RUN([
/*
 * Just see if we can compile/link with popt
 */
#include <popt.h>

int main(int argc, const char *argv[])
{
	struct poptOption popt_options[] = {
		POPT_TABLEEND
	};
	poptGetContext(argv[0], argc, argv, popt_options, POPT_CONTEXT_KEEP_FIRST);

	return 0;
}
], ac_cv_have_popt=yes, ac_cv_have_popt=no,
   [echo $ac_n "compile with POPT. Assuming OK... $ac_c"
    ac_cv_have_popt=yes])
CFLAGS="$ac_save_CFLAGS"
LIBS="$ac_save_LIBS"
if test "$ac_cv_have_popt" = yes ; then
  AC_MSG_RESULT(yes)
else
  AC_MSG_RESULT(no)
  AC_MSG_NOTICE(You need libpopt to compile DBENCH. Install the libpopt-dev package.)
  exit
fi

#
# Check that libsmbclient is available
#
AC_MSG_CHECKING(whether libsmbclient is available)
ac_save_CFLAGS="$CFLAGS"
ac_save_LIBS="$LIBS"
CFLAGS="$CFLAGS $GLIB_CFLAGS"
LIBS="$GLIB_LIBS $LIBS -lsmbclient"
AC_TRY_RUN([
#include <stdio.h>
#include <libsmbclient.h>

int main(int argc, char *argv[])
{
	SMBCCTX *ctx;

	ctx = smbc_new_context();
	if (ctx == NULL) {
		fprintf(stderr, "Could not allocate SMB Context\n");
		return 1;
	}

	smbc_free_context(ctx, 1);

	return 0;
}
], ac_cv_have_libsmbclient=yes, ac_cv_have_libsmbclient=no,
   [echo $ac_n "compile with LIBSMBCLIENT. Assuming OK... $ac_c"
    ac_cv_have_libsmbclient=yes])
CFLAGS="$ac_save_CFLAGS"
LIBS="$ac_save_LIBS"
if test "$ac_cv_have_libsmbclient" = yes ; then
  AC_MSG_RESULT(yes)
  AC_DEFINE(HAVE_LIBSMBCLIENT, 1, [Whether we have LIBSMBCLIENT support])
  LIBSMBCLIENT="-lsmbclient"
  SMBO="smb.o"
else
  AC_MSG_RESULT(no)
  AC_MSG_NOTICE(Libsmbclient-dev not available. Building DBENCH without SMB support)
  LIBSMBCLIENT=""
  SMBO=""
fi

#
# Check that libiscsi is available
#
AC_MSG_CHECKING(whether libiscsi is available)
ac_save_CFLAGS="$CFLAGS"
ac_save_LIBS="$LIBS"
CFLAGS="$CFLAGS $GLIB_CFLAGS"
LIBS="$GLIB_LIBS $LIBS -liscsi"
AC_TRY_RUN([
#include <stdio.h>
#include <iscsi/iscsi.h>

int main(int argc, char *argv[])
{
	struct iscsi_context *iscsi;

	iscsi = iscsi_create_context("iqn.2002-10.com.ronnie:client");
	if (iscsi == NULL) {
		printf("Failed to create iscsi context\n");
		exit(10);
	}

	iscsi_destroy_context(iscsi);

	return 0;
}
], ac_cv_have_libiscsi=yes, ac_cv_have_libiscsi=no,
   [echo $ac_n "compile with LIBISCSI. Assuming OK... $ac_c"
    ac_cv_have_libiscsi=yes])
CFLAGS="$ac_save_CFLAGS"
LIBS="$ac_save_LIBS"
if test "$ac_cv_have_libiscsi" = yes ; then
  AC_MSG_RESULT(yes)
  AC_DEFINE(HAVE_LIBISCSI, 1, [Whether we have LIBISCSI support])
  LIBISCSI="-liscsi"
else
  AC_MSG_RESULT(no)
  AC_MSG_NOTICE(Libiscsi not available. Building DBENCH with old iscsi support)
  LIBISCSI=""
fi

#
# Check whether we have access to Linux SCSI Generic ioctl()
#
AC_MSG_CHECKING(whether Linux SCSI Generic is available)
ac_save_CFLAGS="$CFLAGS"
ac_save_LIBS="$LIBS"
CFLAGS="$CFLAGS $GLIB_CFLAGS"
LIBS="$GLIB_LIBS $LIBS"
AC_TRY_RUN([
/* We can only check that this program compiles. We cant check whether the
   actual ioctl works since opening the device and doing i/o can only be
   done by root.
*/
#include <stdio.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <scsi/sg.h>

int main(int argc, char *argv[])
{
	const char *dev="/dev/sda";
	int fd, vers;

	if((fd=open(dev, O_RDWR))<0){
		return 0;
	}
	if ((ioctl(fd, SG_GET_VERSION_NUM, &vers) < 0) || (vers < 30000)) {
		close(fd);
		return 0;
	}
	close(fd);

	return 0;
}
], ac_cv_linux_scsi_sg=yes, ac_cv_linux_scsi_sg=no,
   [echo $ac_n "cross compiling; assumed OK... $ac_c"
    ac_cv_linux_scsi_sg=yes])
CFLAGS="$ac_save_CFLAGS"
LIBS="$ac_save_LIBS"
if test "$ac_cv_linux_scsi_sg" = yes ; then
  AC_MSG_RESULT(yes)
  have_linux_scsi_sg=yes
  LINUXSCSI="linux_scsi.o"
else
  AC_MSG_RESULT(no)
  have_linux_scsi_sg=no
  LINUXSCSI=""
fi
if test x$have_linux_scsi_sg = xyes
then
  AC_DEFINE(HAVE_LINUX_SCSI_SG, 1, [Define if Linux SCSI Generic is enabled])
fi

AC_SUBST(LIBSMBCLIENT)
AC_SUBST(LIBISCSI)
AC_SUBST(SMBO)
AC_SUBST(LINUXSCSI)
AC_CONFIG_FILES([Makefile])
AC_OUTPUT



AC_MSG_NOTICE(============================================)
if test "$ac_cv_have_libsmbclient" = yes ; then
  AC_MSG_NOTICE(SMB support .......................... [YES])
else
  AC_MSG_NOTICE(SMB support .......................... [NO])
fi
if test "$ac_cv_have_libiscsi" = yes ; then
  AC_MSG_NOTICE(LIBISCSI support ..................... [YES])
else
  AC_MSG_NOTICE(LIBISCSI support ..................... [NO])
fi
if test "$ac_cv_linux_scsi_sg" = yes ; then
  AC_MSG_NOTICE(LINUX SCSI SG support ................ [YES])
else
  AC_MSG_NOTICE(LINUX SCSI SG support ................ [NO])
fi
