/*
 * CURSOR/DEFS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/cursor/defs.h,v 1.13 2003/05/09 05:57:28 dillon Exp $
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <ctype.h>

#define Prototype 	extern
#define Export		extern

#include "libsupport/export.h"
#include "libthreads/export.h"
#include "lex.h"
#include "query.h"
#include "drd_cursor-protos.h"
