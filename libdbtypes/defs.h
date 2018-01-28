/*
 * LIBDBTYPES/DEFS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbtypes/defs.h,v 1.2 2002/08/20 22:05:53 dillon Exp $
 */

#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <dirent.h>

#define Prototype	extern
#define Export		extern

#include "libsupport/export.h"
#include "export.h"
#include "dbtypes-protos.h"

