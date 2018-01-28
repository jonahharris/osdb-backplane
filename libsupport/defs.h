/*
 * DEFS.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/defs.h,v 1.22 2003/04/02 05:27:13 dillon Exp $
 */

#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <pwd.h>
#include <grp.h>
#include <limits.h>
#ifdef sun
#include <strings.h>
#endif
#ifdef linux
#include <sys/time.h>
#include <time.h>
#endif

#include <errno.h>
#include <setjmp.h>

#include <syslog.h>

#define Prototype	extern
#define Export		extern

#define DEFAULT_DBDIR   BACKPLANE_BASE "drdbms"

#include "export.h"
#include "support-protos.h"

