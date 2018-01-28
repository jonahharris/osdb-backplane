/*
 * SOFTINT.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/softint.c,v 1.11 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

Prototype void initSoftInt(softint_t si, void *func);
Prototype void setSoftIntFunc(softint_t si, void *func);
Prototype void setSoftIntPri(SoftInt *si, int pri);
Prototype void issueSoftInt(softint_t si);
Prototype void abortSoftInt(softint_t si);
Prototype int waitSoftInt(softint_t si);

Prototype void runSoftInts(void);

void _defaultSysFunc(SoftInt *si, int force);
void _queueSoftInt(SoftInt *si);

/*
 * Initialize a new software interrupt
 */

void
initSoftInt(SoftInt *si, void *func)
{
    si->si_SysFunc = (func) ? func : _defaultSysFunc;
}

/*
 * setSoftFunc() -	Change the system function for a softint
 *
 *	Note: The software interrupt will not be reissued.
 */

void
setSoftIntFunc(SoftInt *si, void *func)
{
    if (func == NULL)
	func = _defaultSysFunc;
    si->si_SysFunc = func;
}

/*
 * setSoftPri() -	Change the SIPL of a softint, requeue if necessary
 *
 *	Set the SIPL for a softint.  If the software interrupt is queued
 *	and pending, this reorders the softint in the queue and may 
 *	result in an immediate dispatch.
 *
 *	This function will also assign a task to the softint and must be
 *	called if an asynchronous user vector is wanted.
 */

void
setSoftIntPri(SoftInt *si, int pri)
{
    si->si_SIpl = pri;
    si->si_Task = CURTASK;
    if (si->si_Flags & SIF_COMPLETE) {
	if (si->si_Flags & SIF_QUEUED) {
	    removeNode(&si->si_Node);
	    si->si_Flags &= ~SIF_QUEUED;
	}
	_queueSoftInt(si);
    }
}

/*
 * issueSoftInt() -	issue a software interrupt (set completion)
 *
 *	Issue a software interrupt.  If the software interrupt is
 *	already pending (SIF_COMPLETE set), this call is a NOP.
 *
 *	If the software interrupt is not pending then it must be
 *	running (SIF_RUNNING) in order to be able to issue it.
 *
 *	If the softint is SIF_QUEUED, it will be dequeued.  SIF_RUNNING
 *	is cleared and SIF_COMPLETE is set.
 *
 *	If the softint is asynchronous (not in syncwait or aborted), it
 *	will be queued for dispatch.  An immediate dispatch may occur
 *	depending.
 */

void
issueSoftInt(SoftInt *si)
{
    if (si->si_Flags & SIF_COMPLETE)
	return;
    DBASSERT(si->si_Flags & SIF_RUNNING);

    si->si_Flags &= ~SIF_RUNNING;
    if (si->si_Flags & SIF_QUEUED) {
	removeNode(&si->si_Node);
	si->si_Flags &= ~SIF_QUEUED;
    }

    si->si_Flags |= SIF_COMPLETE;
    if (si->si_Flags & (SIF_SYNCWAIT|SIF_ABORTED)) {
	if (si->si_Task)
	    taskWakeup(si->si_Task);
    } else if (si->si_Task) {
	_queueSoftInt(si);
    }
}

/*
 * abortSoftInt() -	Abort an operation
 *
 *	Abort an operation in progress by setting the abort flag and
 *	calling the system function.  The system function should 
 *	issue the software interrupt as soon a possible.
 *
 *	This call has no effect if the operation has already been aborted.
 */

void
abortSoftInt(SoftInt *si)
{
    if (si->si_Flags & SIF_ABORTED)
	return;
    if ((si->si_Flags & (SIF_RUNNING | SIF_COMPLETE)) == 0)
	return;

    si->si_Flags |= SIF_ABORTED;

    if (si->si_Flags & SIF_RUNNING) {
	/*
	 * Still running, call the system function with the abort flag set
	 * and the complete flag left clear (force argument not used in this
	 * case, set to 0).
	 */
	si->si_SysFunc(si, 0);
    } else {
	/*
	 * Already completed (late abort), remove from dispatch queue
	 * if necessary.
	 */
	if (si->si_Flags & SIF_QUEUED) {
	    removeNode(&si->si_Node);
	    si->si_Flags &= ~SIF_QUEUED;
	}
    }
}

/*
 * waitSoftInt() -	Synchronously wait for completion or abort
 *
 *	Wait for the operation to complete synchronously.
 *
 *	No dispatch will occur
 */

int
waitSoftInt(SoftInt *si)
{
    /*
     * Set SYNCWAIT and, if the operation has already completed, make sure
     * it is not queued for dispatch.
     */
    si->si_Flags |= SIF_SYNCWAIT;
    if (si->si_Flags & SIF_COMPLETE) {
	if (si->si_Flags & SIF_QUEUED) {
	    removeNode(&si->si_Node);
	    si->si_Flags &= ~SIF_QUEUED;
	}
    }

    /*
     * Block until operation is no longer running
     */
again:
    si->si_Task = CURTASK;
    while (si->si_Flags & SIF_RUNNING)
	taskWait();

    /*
     * Call SysFunc ourselves.  Don't bother if it has already been called
     * (abort doesn't count).  Call with the force argument set to 1, meaning
     * that this is being called from a synchronous wait.
     */
    if (si->si_Flags & SIF_COMPLETE) {
	si->si_SysFunc(si, 1);
	if (si->si_Flags & (SIF_RUNNING | SIF_COMPLETE))
	    goto again;
    }
    si->si_Task = NULL;

    /*
     * Clear the synchronous wait flag and return the result code.
     */
    si->si_Flags &= ~SIF_SYNCWAIT;
    return(si->si_Error);
}

/*
 * runSoftInts() -	Run any dispatchable pending software interrupts
 *
 *	Pending software interrupts are queued in priority order (highest
 *	priority first).  Software interrupts are run highest-priority-first
 *	as long as pending softints are available and have a higher
 *	software interrupt priority (sipl) then the task's current sipl.
 */

void
runSoftInts(void)
{
    Task *task = CURTASK;
    SoftInt *si;
    int saveIpl;

    /*
     * Save the current task's Ipl, run pending software interrupts,
     * restore the Ipl.
     */
    saveIpl = task->ta_SIpl;

    while ((si = ta_SoftIntPend(task))->si_SIpl > saveIpl) {
	/*
	 * Dequeue the software interrupt, set the SIpl appropriately,
	 * and execute the system function.  The softint must have the
	 * COMPLETE flag set.
	 *
	 * Call the system function with the force flag set to 0, allowing
	 * the system function to defer completion if it wishes (e.g. if
	 * there is a user vector which is set to NULL).
	 */
	removeNode(&si->si_Node);
	si->si_Flags &= ~SIF_QUEUED;

	DBASSERT(si->si_Flags & SIF_COMPLETE);

	task->ta_SIpl = si->si_SIpl;
	si->si_SysFunc(si, 0);
    }
    task->ta_SIpl = saveIpl;
}

/*
 * The default system function for a softint dispatch 
 */
void
_defaultSysFunc(SoftInt *si, int force)
{
    if (si->si_Flags & SIF_COMPLETE) {
	/*
	 * Completion
	 */
	si->si_Flags &= ~SIF_COMPLETE;
    } else {
	/*
	 * Aborting, simply issue the softint.
	 */
	DBASSERT(si->si_Flags & SIF_ABORTED);
	issueSoftInt(si);
    }
}

/*
 * _queueSoftInt()
 */

void
_queueSoftInt(SoftInt *si)
{
    SoftInt *scan;
    Task *task = si->si_Task;

    scan = getSucc(&task->ta_SoftTerm.si_Node);
    while (si->si_SIpl <= scan->si_SIpl)
	scan = getSucc(&scan->si_Node);
    insertNodeBefore(&scan->si_Node, &si->si_Node);
    si->si_Flags |= SIF_QUEUED;

    if (si->si_SIpl > task->ta_SIpl) {
	taskWakeup(task);
	if (task == CURTASK)
	    runSoftInts();
    }
}

