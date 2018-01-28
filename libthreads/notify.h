/*
 * NOTIFY.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/notify.h,v 1.5 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct SoftNotify {
    SoftInt	sn_SoftInt;
    SoftTimer	sn_Timer;
    void	(*sn_UserFunc)(void *data);
    void	*sn_UserData;
} SoftNotify;

#define sn_Node		sn_SoftInt.si_Node
#define sn_Flags	sn_SoftInt.si_Flags

