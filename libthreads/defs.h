/*
 * DEFS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/defs.h,v 1.18 2003/04/02 05:27:15 dillon Exp $
 */

#ifdef __FreeBSD__
#define USE_KQUEUE	1
#else
#define USE_KQUEUE	0
#endif

#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#if USE_KQUEUE
#include <sys/event.h>
#endif
#include <sys/time.h>
#include <sys/socket.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stddef.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#ifdef sun
#include <strings.h>
#endif

#include <errno.h>
#include <setjmp.h>

#define Prototype	extern
#define Export		extern

#include "libsupport/export.h"
#include "export.h"
#include "softint.h"
#include "timer.h"
#include "notify.h"
#include "tlock.h"
#include "io.h"
#include "queue.h"
#include "tasks.h"
#include "threads-protos.h"

/*
 * Machines that cannot handle sigsetjmp/siglongjmp (a version
 * of setjmp/longjmp that does not save/restore the signal mask)
 */
#if defined(sun) || defined(linux)
#ifndef sigsetjmp
#define sigsetjmp(env,savesigs)	setjmp(env)
#define siglongjmp(env, rv)	longjmp(env, rv)
#endif
#endif

