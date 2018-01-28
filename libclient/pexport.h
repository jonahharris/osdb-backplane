/*
 * LIBCLIENT/PEXPORT.H	- Public headers for libclient
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/pexport.h,v 1.3 2003/04/27 20:28:08 dillon Exp $
 */

struct CLDataBase;
struct CLInstance;
struct CLRes;
struct CLRow;

typedef struct CLDataBase *database_t;
typedef struct CLInstance *instance_t;
typedef struct CLRes *res_t;
typedef struct CLRow *row_t;
typedef u_int32_t	cluser_t;

#define CLTYPE_RO	1
#define CLTYPE_RW	2

#define CL_STALL_COUNT	65536

#include "clientmsg.h"

#include <pthread.h>

#define IOFD_NULL	-1

typedef int iofd_t;
typedef int notify_t;

typedef struct TLock {
    pthread_mutex_t	mutex;
} *tlock_t;

#include "libpclient/pclient-exports.h"

