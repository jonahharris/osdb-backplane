/*
 * FLOCK.C	- Thread/Process locks
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/flock.c,v 1.5 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

Export void t_flock_init(FLock *fl);
Export void t_flock_ex(FLock *fl);
Export void t_flock_sh(FLock *fl);
Export void t_flock_un(FLock *fl);

#ifdef sun
/* FLOCK FUNCTIONS */

#else

void
t_flock_init(FLock *fl)
{
    bzero(fl, sizeof(FLock));
    fl->fl_Fd = -1;
    initList(&fl->fl_List);
}

void
t_flock_ex(FLock *fl)
{
    while (fl->fl_Count != 0)
	taskWaitOnList(&fl->fl_List);
    if (fl->fl_Count == 0)
	flock(fl->fl_Fd, LOCK_EX);
    --fl->fl_Count;
}

void
t_flock_sh(FLock *fl)
{
    while (fl->fl_Count < 0)
	taskWaitOnList(&fl->fl_List);
    if (fl->fl_Count == 0)
	flock(fl->fl_Fd, LOCK_SH);
    ++fl->fl_Count;
}

void
t_flock_un(FLock *fl)
{
    DBASSERT(fl->fl_Count);

    if (fl->fl_Count > 0) {
	--fl->fl_Count;
    } else {
	++fl->fl_Count;
    }
    if (fl->fl_Count == 0) {
	flock(fl->fl_Fd, LOCK_UN);
	taskWakeupList(&fl->fl_List);
    }
}

#endif


