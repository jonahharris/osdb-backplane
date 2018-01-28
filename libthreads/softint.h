/*
 * SOFTINT.H	- Software Interrupts
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/softint.h,v 1.5 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct SoftInt {
    Node	si_Node;		/* linked list */
    struct Task *si_Task;		/* thread owning softint */
    int		si_SIpl;		/* software interrupt priority */
    int		si_Flags;		/* sequencing flags */
    void	(*si_SysFunc)(struct SoftInt *si, int force);
    int		si_Error;
} SoftInt;

#define SIF_RUNNING	0x0001		/* operation running */
#define SIF_COMPLETE	0x0002		/* operation complete */
#define SIF_QUEUED	0x0004		/* queued on some list */
#define SIF_ABORTED	0x0008		/* abort flag (forces synchronous) */
#define SIF_SYNCWAIT	0x0010		/* synchronous wait */

#define NT_SOFTINT	1
#define NT_TIMER	2
#define NT_DESC		3
#define NT_TASK		4
#define NT_QUEUER	5
#define NT_QUEUEW	6

