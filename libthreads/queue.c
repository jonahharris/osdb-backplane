/*
 * QUEUE.C	- Data queueing between threads
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/queue.c,v 1.10 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

Export squeue_t allocQueue(void);
Export squeue_t dupQueue(squeue_t sq);
Export void freeQueue(squeue_t sq);
Export void abortQueue(squeue_t sq);

Export void *readQueue(squeue_t sq);
Export void *readQueue_nb(squeue_t sq);
Export void readQueueA(squeue_t sq);
Export void writeQueue(squeue_t sq, void *pktData);

void _softQueueInt(SoftQueue *sq, int force);

static SoftQueue *
_dupQueue(HardQueue *hq)
{
    SoftQueue *sq = zalloc(sizeof(SoftQueue));

    initSoftInt(&sq->sq_SoftInt, _softQueueInt);
    sq->sq_Hq = hq;
    ++hq->hq_Refs;
    return(sq);
}

SoftQueue *
allocQueue(void)
{
    HardQueue *hq = zalloc(sizeof(HardQueue));

    initList(&hq->hq_SqList);
    initList(&hq->hq_PkList);
    return(_dupQueue(hq));
}

SoftQueue *
dupQueue(SoftQueue *sq)
{
    return(_dupQueue(sq->sq_Hq));
}

void
freeQueue(SoftQueue *sq)
{
    HardQueue *hq = sq->sq_Hq;

    DBASSERT(hq->hq_Refs > 0);

    abortSoftInt(&sq->sq_SoftInt);
    waitSoftInt(&sq->sq_SoftInt);

    if (--hq->hq_Refs == 0) {
	HQPkt *pkt;

	while ((pkt = remHead(&hq->hq_PkList)) != NULL)
	    zfree(pkt, sizeof(HQPkt));
	zfree(hq, sizeof(HardQueue));
    }
    sq->sq_Hq = NULL;
    zfree(sq, sizeof(SoftQueue));
}

void 
abortQueue(squeue_t sq)
{
    abortSoftInt(&sq->sq_SoftInt);
}

void *
readQueue(SoftQueue *sq)
{
    HardQueue *hq = sq->sq_Hq;
    void *pktData;

    DBASSERT((sq->sq_Flags & (SIF_RUNNING|SIF_COMPLETE)) == 0);

    if ((pktData = readQueue_nb(sq)) == NULL) {
	addTail(&hq->hq_SqList, &sq->sq_Node);
	sq->sq_Flags |= SIF_RUNNING | SIF_QUEUED;
	sq->sq_PktData = NULL;
	waitSoftInt(&sq->sq_SoftInt);
	pktData = sq->sq_PktData;
    }
    return(pktData);
}

void *
readQueue_nb(SoftQueue *sq)
{
    HardQueue *hq = sq->sq_Hq;
    HQPkt *pkt;
    void *pktData;

    if ((pkt = remHead(&hq->hq_PkList)) == NULL)
	return(NULL);
    pktData = pkt->hpk_Data;
    zfree(pkt, sizeof(HQPkt));
    return(pktData);
}

void
readQueueA(SoftQueue *sq)
{
    void *pktData;
    HardQueue *hq = sq->sq_Hq;

    DBASSERT((sq->sq_Flags & (SIF_RUNNING|SIF_COMPLETE)) == 0);

    if ((pktData = readQueue_nb(sq)) == NULL) {
	addTail(&hq->hq_SqList, &sq->sq_Node);
	sq->sq_SoftInt.si_Task = CURTASK;
	sq->sq_Flags |= SIF_RUNNING | SIF_QUEUED;
	sq->sq_PktData = NULL;
    } else {
	sq->sq_Flags |= SIF_RUNNING;
	sq->sq_PktData = pktData;
	sq->sq_SoftInt.si_Task = CURTASK;
	issueSoftInt(&sq->sq_SoftInt);
    }
}

void
writeQueue(SoftQueue *sq, void *pktData)
{
    HardQueue *hq = sq->sq_Hq;
    SoftQueue *sqr;

    if ((sqr = remHead(&hq->hq_SqList)) != NULL) {
	sqr->sq_PktData = pktData;
	sqr->sq_Flags &= ~SIF_QUEUED;
	issueSoftInt(&sqr->sq_SoftInt);
    } else {
	HQPkt *hpk = zalloc(sizeof(HQPkt));
	hpk->hpk_Data = pktData;
	addTail(&hq->hq_PkList, &hpk->hpk_Node);
    }
}

void
_softQueueInt(SoftQueue *sq, int force)
{
    if (sq->sq_Flags & SIF_COMPLETE) {
	/*
	 * Completion Processing
	 */
	int dispatch = 0;

	if (sq->sq_UserFunc &&
	    (sq->sq_Flags & (SIF_ABORTED | SIF_SYNCWAIT)) == 0
	) {
	    dispatch = 1;
	}

	if (force || dispatch)
	    sq->sq_Flags &= ~SIF_COMPLETE;
	if (dispatch)
	    sq->sq_UserFunc(sq->sq_UserData, sq->sq_PktData);
    } else {
	/*
	 * Abort Processing
	 */
	DBASSERT(sq->sq_Flags & SIF_ABORTED);
	DBASSERT(sq->sq_Flags & SIF_RUNNING);
	issueSoftInt(&sq->sq_SoftInt);
    }
}

