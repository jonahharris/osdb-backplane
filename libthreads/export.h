/*
 * EXPORT.H	- included by others to access this library's API
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/export.h,v 1.16 2003/05/09 05:57:36 dillon Exp $
 */

struct Task;
struct IOFd;
struct SoftQueue;
struct SoftInt;
struct SoftTimer;
struct TLock;

typedef struct Task *bkpl_task_t;
typedef struct IOFd *iofd_t;
typedef struct SoftNotify *notify_t;
typedef struct SoftQueue *squeue_t;
typedef struct SoftInt *softint_t;
typedef struct SoftTimer *softtimer_t;
typedef struct TLock *tlock_t;

typedef struct FLock {
    int		fl_Fd;		/* descriptor */
    int		fl_Count;	/* lock count */
    List	fl_List;	/* waiting tasks */
} FLock;

typedef void (*si_func_t)(void *data);
typedef void (*st_func_t)(void *data);
typedef void (*io_func_t)(iofd_t io, void *buf, int r);

#define IOFD_NULL		NULL

#define DTHREADS_VERSION	4

#include "libthreads/threads-exports.h"

/*
 * Cooperative multitasking - quick check/switch routine
 */
static __inline void
taskQuantum(void)
{
    if (TaskQuantum <= 0 || IOQuantum <= 0)
	taskGiveup();
}

