/*
 * TASKS.H	- Internal threads library structures
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libthreads/tasks.h,v 1.7 2002/08/20 22:05:57 dillon Exp $
 */

typedef struct Task {
    Node	ta_Node;
    int		ta_SIpl;
    int		ta_Flags;
    void	(*ta_StartFunc)(void *data);
    void	*ta_StartData;
    SoftInt	ta_SoftInt;		/* run/wait q, current pri, start */
    SoftInt	ta_SoftTerm;		/* softint terminator / exit vect */
    char	*ta_Stack;		/* stack */
    int		ta_StackSize;
    int		ta_SoftCount;		/* softints in progress */
    int		ta_SchedAccum;
    int		ta_SchedPri;
    sigjmp_buf	ta_Env;			/* wakeup longjmp */
} Task;

#define ta_SoftIntPend(task)	((SoftInt *)(task)->ta_SoftTerm.si_Node.no_Next)

#define TAF_RUNNING	0x0001
#define TAF_QUEUED	0x0002
#define TAF_EXITED	0x0004

#define SIPL_TIMER	125

#define TASK_QUANTUM_MIN	-10
#define TASK_QUANTUM_MAX	2

#define CURTASK ((Task *)TaskRun.ta_Node.no_Next)
#define NEXTTASK ((Task *)CURTASK->ta_Node.no_Next)

