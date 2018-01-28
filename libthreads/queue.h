/*
 * QUEUE.H	- Internal threads library structures
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/queue.h,v 1.5 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct HQPkt {
    Node	hpk_Node;
    void	*hpk_Data;
} HQPkt;

typedef struct HardQueue {
    List	hq_SqList;	/* softqueues waiting for data */
    List	hq_PkList;	/* queued packets */
    int		hq_Refs;	/* ref count */
} HardQueue;

typedef struct SoftQueue {
    SoftInt	sq_SoftInt;
    void	(*sq_UserFunc)(void *data, void *pkt);
    void	*sq_UserData;
    void	*sq_PktData;
    int		sq_How;
    HardQueue	*sq_Hq;
} SoftQueue;

#define sq_Node		sq_SoftInt.si_Node
#define sq_Flags	sq_SoftInt.si_Flags

