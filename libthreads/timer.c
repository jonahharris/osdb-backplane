/*
 * TIMER.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/timer.c,v 1.9 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

Export softtimer_t allocTimer(int ms, int incms);
Export void freeTimer(softtimer_t st);
Export void setTimerTimeout(softtimer_t st, int ms, int incms);
Export void setTimerDispatch(softtimer_t st, void *func, void *data, int pri);
Export void startTimer(softtimer_t st);
Export void waitTimer(softtimer_t st);
Export void abortTimer(softtimer_t st);
Export void clearTimer(softtimer_t st);
Export void reloadTimer(softtimer_t st);
Export struct timeval *testTimers(struct timeval *tv);

Prototype void softTimerInt(softtimer_t st, int force);

Prototype void initTimer(SoftTimer *st, int ms, int incms);

Prototype SoftTimer TimerRun;

SoftTimer TimerRun = { { INITCNODE(TimerRun.st_Node) } };

/*
 * initTimer() -	initialize a timer given a preallocated structure
 *
 *	Initialize a new timer.  The default is to create a timer with an
 *	initial timeout of MS.  If INCMS is 0, this will be a oneshot timer.
 *	If INCMS is non-zero, this will be a periodic timer whos period after
 *	the initial MS timeout is INCMS (all times are in milliseconds).
 *
 *	The user function is left NULL unless set, so the default is for
 *	synchronous operation.
 */
void
initTimer(SoftTimer *st, int ms, int incms)
{
    initSoftInt(&st->st_SoftInt, softTimerInt);
    if (ms || incms)
	setTimerTimeout(st, ms, incms);
}

/*
 * setTimerTimeout() - initialize a timer structure with a specified timeout
 */
void
setTimerTimeout(SoftTimer *st, int ms, int incms)
{
    DBASSERT((st->st_Flags & SIF_RUNNING) == 0);

    gettimeofday(&st->st_Tod, NULL);

    st->st_Tod.tv_usec += (ms % 1000) * 1000;
    st->st_Tod.tv_sec += ms / 1000;
    if (st->st_Tod.tv_usec > 1000000) {
	st->st_Tod.tv_usec -= 1000000;
	++st->st_Tod.tv_sec;
    }
    if (incms) {
	st->st_Inc.tv_usec = (incms % 1000) * 1000;
	st->st_Inc.tv_sec = incms / 1000;
    } else {
	st->st_Inc.tv_usec = 0;
	st->st_Inc.tv_sec = 0;
    }
}

/*
 * allocTimer() - allocate and initialize a new timer
 */

SoftTimer *
allocTimer(int ms, int incms)
{
    SoftTimer *st = zalloc(sizeof(SoftTimer));

    initTimer(st, ms, incms);
    return(st);
}

/*
 * freeTimer() - deallocate a timer, abort if active.
 */

void
freeTimer(SoftTimer *st)
{
    abortTimer(st);
    waitTimer(st);
    zfree(st, sizeof(SoftTimer));
}

/*
 * setTimerDispatch() - set dispatch (may cause immediate interrupt)
 *
 *	This function may cause an immediate interrupt if a pending
 *	timer interrupt's function goes from NULL to non-NULL.
 */

void
setTimerDispatch(SoftTimer *st, void *func, void *data, int pri)
{
    st->st_UserFunc = func;
    st->st_UserData = data;
    setSoftIntPri(&st->st_SoftInt, pri);
}

/*
 * startTimer()	- start a timer running
 *
 *	This function has no effect if the timer is already running or 
 *	pending.  We scan from both sides of the timer queue to optimize
 *	both short and long timeouts.
 */

void
startTimer(SoftTimer *st)
{
    SoftTimer *sfwd;
    SoftTimer *sbak;

    if (st->st_Flags & (SIF_RUNNING | SIF_COMPLETE))
	return;

    /*
     * Clear any prior abort status on idle timer
     */
    st->st_Flags &= ~SIF_ABORTED;
    st->st_SoftInt.si_Task = CURTASK;

    /*
     * Queue the timer and set it running.
     */

    sfwd = getSucc(&TimerRun.st_Node);
    sbak = getPred(&TimerRun.st_Node);

    while (sfwd != &TimerRun) {
	if (st->st_Tod.tv_sec < sfwd->st_Tod.tv_sec ||
	    (st->st_Tod.tv_sec == sfwd->st_Tod.tv_sec &&
	      st->st_Tod.tv_usec < sfwd->st_Tod.tv_usec)
	) {
	    break;
	}
	if (st->st_Tod.tv_sec > sbak->st_Tod.tv_sec ||
	    (st->st_Tod.tv_sec == sbak->st_Tod.tv_sec &&
	      st->st_Tod.tv_usec >= sbak->st_Tod.tv_usec)
	) {
	    insertNodeAfter(&sbak->st_Node, &st->st_Node);
	    st->st_Flags |= SIF_QUEUED | SIF_RUNNING;
	    return;
	}
	sfwd = getSucc(&sfwd->st_Node);
	sbak = getPred(&sbak->st_Node);
    }
    insertNodeBefore(&sfwd->st_Node, &st->st_Node);
    st->st_Flags |= SIF_QUEUED | SIF_RUNNING;
}

/*
 * reloadTimer() - Reload a periodic timer with the next interval
 *
 *	This function has no effect if the timer is already running or
 *	pending.  The reload is supposed to take place just prior to
 *	any dispatch.
 */

void 
reloadTimer(SoftTimer *st)
{
    if ((st->st_Flags & (SIF_RUNNING | SIF_COMPLETE)) == 0) {
	st->st_Tod.tv_usec += st->st_Inc.tv_usec;
	st->st_Tod.tv_sec += st->st_Inc.tv_sec;
	if (st->st_Tod.tv_usec > 1000000) {
	    st->st_Tod.tv_usec -= 1000000;
	    ++st->st_Tod.tv_sec;
	}
	startTimer(st);
    }
}

/*
 * SoftTimerInt() - timer softint system function
 *
 *	The timer softint is responsible for reloading a periodic timer,
 *	and running the optional user-supplied function.
 */

void 
softTimerInt(SoftTimer *st, int force)
{
    if (st->st_Flags & SIF_COMPLETE) {
	/*
	 * Completion Processing
	 */
	int dispatch = 0;

	if (st->st_UserFunc && 
	    (st->st_Flags & (SIF_ABORTED | SIF_SYNCWAIT)) == 0
	) {
	    dispatch = 1;
	}

	if (force || dispatch) {
	    st->st_Flags &= ~SIF_COMPLETE;
	    if ((st->st_Flags & SIF_ABORTED) == 0 &&
		(st->st_Inc.tv_usec || st->st_Inc.tv_sec)
	    ) {
		reloadTimer(st);
	    } else {
		st->st_SoftInt.si_Task = NULL;
	    }
	}
	if (dispatch)
	    st->st_UserFunc(st->st_UserData);
    } else {
	/*
	 * Abort Processing
	 */
	DBASSERT(st->st_Flags & SIF_ABORTED);
	DBASSERT(st->st_Flags & SIF_RUNNING);
	issueSoftInt(&st->st_SoftInt);
    }
}

void 
abortTimer(SoftTimer *st)
{
    abortSoftInt(&st->st_SoftInt);
}

/*
 * waitTimer() - Wait for a timer to complete.
 *
 *	This function synchronously waits for a timer to complete, executes
 *	the system function (if any), and returns.  Any vector associated 
 *	with the timer is temporarily disabled.
 *
 *	If the timer is not active this function returns immediately.
 */

void
waitTimer(SoftTimer *st)
{
    waitSoftInt(&st->st_SoftInt);
}

/*
 * testTimers() - Process timer events, return shortest timeout remaining
 *		  if no events were processed, NULL if at least one event
 *		  was processed.
 */

struct timeval *
testTimers(struct timeval *tv)
{
    SoftTimer *st;
    int count = 0;

    if (getSucc(&TimerRun.st_Node) == &TimerRun) {
	tv->tv_sec = 10;
	tv->tv_usec = 0;
	return(tv);
    }
    gettimeofday(tv, NULL);
    while ((st = getSucc(&TimerRun.st_Node)) != &TimerRun) {
	if (st->st_Tod.tv_sec > tv->tv_sec ||
	    (st->st_Tod.tv_sec == tv->tv_sec && 
	     st->st_Tod.tv_usec >= tv->tv_usec) 
	) {
	    if (count)
		break;
	    tv->tv_sec = st->st_Tod.tv_sec - tv->tv_sec;
	    tv->tv_usec = st->st_Tod.tv_usec - tv->tv_usec;
	    if ((int)tv->tv_usec < 0) {
		tv->tv_usec += 1000000;
		if (--tv->tv_sec < 0) {		/* problem */
		    tv->tv_sec = 0;
		    tv->tv_usec = 25000;
		}
	    }
	    return(tv);
	}
	issueSoftInt(&st->st_SoftInt);
	++count;
    }
    return(NULL);
}

