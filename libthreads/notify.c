/*
 * NOTIFY.C	- Simple (user) signaling softints
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/notify.c,v 1.6 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

Export notify_t allocNotify(void);
Export void freeNotify(notify_t sn);
Export void setNotifyDispatch(notify_t sn, void *func, void *data, int pri);
Export void issueNotify(notify_t sn);
Export void clearNotify(notify_t sn);
Export int waitNotify(notify_t sn, int ms);

static void _softNotifyInt(SoftNotify *sn, int force);
static void _snTimerInt(SoftNotify *sn);

SoftNotify *
allocNotify(void)
{
    SoftNotify *sn = zalloc(sizeof(SoftNotify));

    initSoftInt(&sn->sn_SoftInt, _softNotifyInt);
    initTimer(&sn->sn_Timer, 0, 0);
    setTimerDispatch(&sn->sn_Timer, _snTimerInt, sn, SIPL_TIMER);
    sn->sn_Flags |= SIF_RUNNING;
    return(sn);
}

void
freeNotify(SoftNotify *sn)
{
    abortSoftInt(&sn->sn_SoftInt);
    waitSoftInt(&sn->sn_SoftInt);
    zfree(sn, sizeof(SoftNotify));
}

void 
issueNotify(SoftNotify *sn)
{
    issueSoftInt(&sn->sn_SoftInt);
}

void
clearNotify(SoftNotify *sn)
{
    if (sn->sn_Flags & SIF_COMPLETE) {
	abortSoftInt(&sn->sn_SoftInt);
	waitSoftInt(&sn->sn_SoftInt);
    }
}

int
waitNotify(SoftNotify *sn, int ms)
{
    int r;

    if (ms) {
	setTimerTimeout(&sn->sn_Timer, ms, 0);
	startTimer(&sn->sn_Timer);
    }
    r = waitSoftInt(&sn->sn_SoftInt);
    if (ms && r != -2) {
	abortTimer(&sn->sn_Timer);
	waitTimer(&sn->sn_Timer);
    }
    sn->sn_SoftInt.si_Error = 0;
    sn->sn_Flags |= SIF_RUNNING;
    return(r);
}

void 
setNotifyDispatch(SoftNotify *sn, void *func, void *data, int pri)
{
    sn->sn_UserFunc = func;
    sn->sn_UserData = data;
    setSoftIntPri(&sn->sn_SoftInt, pri);
}

static void 
_softNotifyInt(SoftNotify *sn, int force)
{
    if (sn->sn_Flags & SIF_COMPLETE) {
	/*
	 * Completion Processing
	 */
	int dispatch = 0;

	if (sn->sn_UserFunc &&
	    (sn->sn_Flags & (SIF_ABORTED | SIF_SYNCWAIT)) == 0
	) {
	    dispatch = 1;
	}

	if (force || dispatch)
	    sn->sn_Flags &= ~SIF_COMPLETE;
	if (dispatch) {
	    sn->sn_Flags |= SIF_RUNNING;
	    sn->sn_UserFunc(sn->sn_UserData);
	}
    } else {
	/*
	 * Abort Processing
	 */
	DBASSERT(sn->sn_Flags & SIF_ABORTED);
	DBASSERT(sn->sn_Flags & SIF_RUNNING);
	issueSoftInt(&sn->sn_SoftInt);
    }
}

static void
_snTimerInt(SoftNotify *sn)
{
    sn->sn_SoftInt.si_Error = -2;
    issueSoftInt(&sn->sn_SoftInt);
}

