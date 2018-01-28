/*
 * LIBCLIENT/EXPORT.H	- Public headers for libclient
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/export.h,v 1.8 2002/08/20 22:05:48 dillon Exp $
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
#include "libclient/client-exports.h"

