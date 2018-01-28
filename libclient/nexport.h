/*
 * LIBCLIENT/PEXPORT.H	- Public headers for libclient
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/nexport.h,v 1.1 2003/04/27 20:28:08 dillon Exp $
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

#define IOFD_NULL	-1

typedef int iofd_t;
typedef int notify_t;

typedef struct TLock {
    int			dummy;
} *tlock_t;

#include "libnclient/nclient-exports.h"

