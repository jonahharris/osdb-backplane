/*
 * DATABASE/DEFS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/database/defs.h,v 1.16 2003/05/09 05:57:29 dillon Exp $
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>

#define Prototype 	extern
#define Export		extern

#include "libsupport/export.h"
#include "libthreads/export.h"
#include "libdbtypes/export.h"
#include "rdbms/errors.h"
#include "libdbcore/export.h"
#include "libdbcore/native.h"
#include "libdbcore/lex.h"
#include "libclient/export.h"
#include "libclient/client.h"
#include "drd_database-protos.h"

#define MAXMERGE_COUNT	SYNCMERGE_COUNT
#define MAXMERGE_BYTES	SYNCMERGE_BYTES

