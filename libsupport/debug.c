/*
 * DEBUG.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Debug helper routines.  Supports debugging to stderr and/or to a file.
 *
 * $Backplane: rdbms/libsupport/debug.c,v 1.12 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"
#include <errno.h>
#include <sys/time.h>

Export void dbprintf(int level, const char *ctl, ...);
Export void dbwarning(const char *ctl, ...);
Export void dberror(const char *ctl, ...);
Export void dberrorsys(const char *ctl, ...);
Export void dbsetfile(const char *fileName);
Export void _dbprintf(const char *ctl, va_list va);
Export void dbinfo(const char *ctl, ...);
Export void dbinfo2(const char *ctl, ...);
Export void dbinfo3(const char *ctl, ...);
Export void dbinfo4(const char *ctl, ...);

int DebugOpt;

static FILE *DebugFo;

/*
 * dbprintf() - debugging output if level <= DebugOpt 
 *
 *	level is typically 1.
 */
void
dbprintf(int level, const char *ctl, ...)
{
    if (level <= DebugOpt) {
	va_list va;

	va_start(va, ctl);
	_dbprintf(ctl, va);
	va_end(va);
    }
}

/*
 * dbinfo() - debugging output if DebugOpt >= 1
 * dbinfo2() - debugging output if DebugOpt >= 2
 * dbinfo3() - debugging output if DebugOpt >= 3
 * dbinfo4() - debugging output if DebugOpt >= 4
 */
void
dbinfo(const char *ctl, ...)
{
    if (DebugOpt) {
	va_list va;

	va_start(va, ctl);
	_dbprintf(ctl, va);
	va_end(va);
    }
}

void
dbinfo2(const char *ctl, ...)
{
    if (DebugOpt >= 2) {
	va_list va;

	va_start(va, ctl);
	_dbprintf(ctl, va);
	va_end(va);
    }
}

void
dbinfo3(const char *ctl, ...)
{
    if (DebugOpt >= 3) {
	va_list va;

	va_start(va, ctl);
	_dbprintf(ctl, va);
	va_end(va);
    }
}

void
dbinfo4(const char *ctl, ...)
{
    if (DebugOpt >= 4) {
	va_list va;

	va_start(va, ctl);
	_dbprintf(ctl, va);
	va_end(va);
    }
}

/*
 * dbwarning()
 *
 *	Generate a debugger warning.  The caller should supply any necessary
 *	newlines in his control string.
 */
void
dbwarning(const char *ctl, ...)
{
    va_list va;

    va_start(va, ctl);
    _dbprintf(ctl, va);
    va_end(va);
}

/*
 * dberror()
 *
 *	Generate a debugger error.  The caller should supply any necessary
 *	newlines in his control string.
 */
void
dberror(const char *ctl, ...)
{
    va_list va;

    va_start(va, ctl);
    _dbprintf(ctl, va);
    va_end(va);
}

/*
 * dberrorsys()
 *
 *	Generate a debugger error along with the system error string for
 *	the current value of errno.  The caller should not supply a newline.
 */
void
dberrorsys(const char *ctl, ...)
{
    char *buf;
    int eno = errno;
    va_list va;

    va_start(va, ctl);
    safe_vasprintf(&buf, ctl, va);
    va_end(va);
    dberror("%s (%s)\n", buf, strerror(eno));
    safe_free(&buf);
}

/*
 * dbsetfile() - set the debugging output file
 *
 *	Set the debug output file to the specified file name. Any previously
 *	open debug output file will be closed.  Passing NULL will close
 *	any previously open file without opening a new one.
 *
 *	The file descriptor will be set to close-on-exec
 */

void
dbsetfile(const char *file)
{
    if (DebugFo) {
	fclose(DebugFo);
	DebugFo = NULL;
    }
    if (file && file[0]) {
	char *path;
	int fd;

	safe_asprintf(&path, "%s.debug", file);
	if ((fd = open(path, O_RDWR|O_APPEND|O_CREAT|O_TRUNC, 0666)) >= 0) {
	    fcntl(fd, F_SETFD, 1);
	    DebugFo = fdopen(fd, "w");
	}
	safe_free(&path);
    }
}

/*
 * _dbprintf() - Internal debugging printf.  See debug.h
 *
 *	Generates formatted debugging output to stderr and, if set, a
 *	debug file.
 */
void
_dbprintf(const char *ctl, va_list va)
{
    static struct timeval ltv;

    vfprintf(stderr, ctl, va);
    if (DebugFo) {
	struct timeval tv;
	int dt = 0;

	gettimeofday(&tv, NULL);
	if (ltv.tv_sec != 0) {
	    dt = tv.tv_usec + 1000000 - ltv.tv_usec +
		(tv.tv_sec - 1 - ltv.tv_sec) * 1000000;
	}
	ltv = tv;
	fprintf(DebugFo, "ms=%3d.%03d - ", dt / 1000, dt % 1000);
	vfprintf(DebugFo, ctl, va);
	fflush(DebugFo);
    }
}

