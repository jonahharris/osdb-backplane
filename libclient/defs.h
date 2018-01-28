/*
 * LIBCLIENT/DEFS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/defs.h,v 1.15 2003/05/09 05:57:31 dillon Exp $
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
#if 0
#if !defined(USE_PTHREAD)
#include "libthreads/export.h"
#endif
#endif
#include "libdbtypes/export.h"
#include "rdbms/errors.h"
#include "libdbcore/types.h"
#if defined(USE_PTHREAD)
#include "pexport.h"
#elif defined(USE_NOTHREAD)
#include "nexport.h"
#else
#include "libthreads/export.h"
#include "export.h"
#endif
#include "client.h"
#if defined(USE_PTHREAD)
#include "pclient-protos.h"
#elif defined(USE_NOTHREAD)
#include "nclient-protos.h"
#else
#include "client-protos.h"
#endif

#ifndef BYTE_ORDER
#error "BYTE_ORDER not defined"
#elif BYTE_ORDER != LITTLE_ENDIAN && BYTE_ORDER != BIG_ENDIAN
#error "Unknown byte ordering during compilation"
#endif

