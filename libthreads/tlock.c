/*
 * TLOCK.C	- Simple (user) locks
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/tlock.c,v 1.4 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

Export tlock_t allocTLock(void);
Export void freeTLock(tlock_t *ptl);
Export void getTLock(tlock_t tl);
Export void relTLock(tlock_t tl);

tlock_t
allocTLock(void)
{
    TLock *tl = zalloc(sizeof(TLock));
    initList(&tl->tl_WaitList);
    return(tl);
}

void
freeTLock(tlock_t *ptl)
{
    tlock_t tl;
    if ((tl = *ptl) != NULL) {
	DBASSERT(tl->tl_Owner == NULL);
	zfree(tl, sizeof(TLock));
	*ptl = NULL;
    }
}

void
getTLock(tlock_t tl)
{
    while (tl->tl_Owner != NULL)
	taskWaitOnList(&tl->tl_WaitList);
    tl->tl_Owner = CURTASK;
}

void
relTLock(tlock_t tl)
{
    DBASSERT(tl->tl_Owner == CURTASK);
    tl->tl_Owner = NULL;
    taskWakeupList1(&tl->tl_WaitList);
}

