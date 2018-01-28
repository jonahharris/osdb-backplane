/*
 * TIMER.H	- Software Timers
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/timer.h,v 1.5 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct SoftTimer {
    SoftInt	st_SoftInt;
    st_func_t   st_UserFunc;		/* interrupt vector */
    void        *st_UserData;		/* interrupt data */
    struct timeval st_Tod;		/* alarm time */
    struct timeval st_Inc;		/* periodic timer if non-zero */
} SoftTimer;

#define st_Node		st_SoftInt.si_Node
#define st_Flags	st_SoftInt.si_Flags

