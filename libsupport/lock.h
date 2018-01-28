/*
 * LOCK.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/lock.h,v 1.3 2002/08/20 22:05:55 dillon Exp $
 *
 *	Tracking structure for locally held posix locks
 */

typedef struct LockTrack {
    Node	lt_Node;
    off_t	lt_Off;
    off_t	lt_Bytes;
} LockTrack;

