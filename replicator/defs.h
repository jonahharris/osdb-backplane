/*
 * REPLICATOR/DEFS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/replicator/defs.h,v 1.16 2003/05/09 05:57:37 dillon Exp $
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
#ifdef linux
#include <sys/file.h>	/* flock */
#include <sys/time.h>	/* gettimeofday */
#include <time.h>	/* time */
#endif
#ifdef __APPLE__
#include <sys/time.h>
#endif

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
#include "pkt.h"
#include "database.h"
#include "dbinfo.h"
#include "link.h"
#include "replicator-protos.h"

#if HAS_MD5DATA
#include <md5.h>
#elif USE_OPENSSL_CRYPTO
#include <openssl/crypto.h>
#else
#include <crypt.h>
#endif

