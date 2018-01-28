/*
 * TLOCK.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/tlock.h,v 1.4 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct TLock {
    List	tl_WaitList;
    struct Task	*tl_Owner;
} TLock;

