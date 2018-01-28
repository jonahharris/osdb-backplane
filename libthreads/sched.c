/*
 * SCHED.C	- Task scheduler
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 *	Implements a synchronous non-preemptive serialized fractional
 *	thread scheduler.  Mouthful!
 *
 *	This is a fractional scheduler, which means that all tasks get
 *	a percentage of cpu based on their ta_SchedPri verses the aggregate
 *	ta_SchedPri of all runnable tasks.
 *
 * $Backplane: rdbms/libthreads/sched.c,v 1.18 2003/05/09 05:57:36 dillon Exp $
 */

#include "defs.h"
#include <signal.h>
#include <sys/socket.h>
#include <sys/mman.h>

Export bkpl_task_t taskCreate(void *func, void *data);
Export void taskDelete(bkpl_task_t task);
Export bkpl_task_t curTask(void);
Export void taskGiveup(void);
Export void taskSleep(int ms);
Export void taskWait(void);
Export void taskWaitOnList(List *list);
Export void taskWakeup(bkpl_task_t task);
Export void taskWakeupList(List *list);
Export void taskWakeupList1(List *list);
Export int raiseSIpl(int sipl);
Export int setSIpl(int sipl);

Export volatile int TaskQuantum;
Export volatile int IOQuantum;

Prototype void taskSched(void);
Prototype void taskInitThreads(void);

Prototype int TaskPageSize;
Prototype Task TaskRun;
Prototype Task TaskExit;

static void taskStartupSig(int sigNo);
static void taskTimerSig(int sigNo);

volatile int TaskQuantum;
volatile int IOQuantum;
Task  TaskRun = { INITCNODE(TaskRun.ta_Node) };
Task  TaskExit = { INITCNODE(TaskExit.ta_Node) };

static int	SchedAggPri;		/* aggregate of runnable priorities */
static int	TaskCount;
static int	TaskStackSize = 128 * 1024;
static int	TaskPgSize;
static volatile int DisableQuantumInt;
static sigjmp_buf StartupEnv;

#define MILLION 1000000

void
taskInitThreads(void)
{
    struct itimerval val = { { 0, MILLION/20}, { 0, MILLION/20 } };

    TaskPgSize = getpagesize();
    signal(SIGVTALRM, taskTimerSig);
    setitimer(ITIMER_VIRTUAL, &val, NULL);
}

Task * 
taskCreate(void *func, void *data)
{
    Task *task;
    struct sigaltstack snew;
    struct sigaltstack sold;
    struct sigaction sa;
    struct sigaction osa;

    task = zalloc(sizeof(Task));

    task->ta_StackSize = TaskStackSize;		/* stack */
#ifdef sun
    task->ta_Stack = malloc(task->ta_StackSize);
#else
    task->ta_Stack = mmap(NULL, task->ta_StackSize, 
			PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
#endif
    task->ta_StartFunc = func;		/* startup func */
    task->ta_StartData = data;
    task->ta_SIpl = 0;
    task->ta_SchedPri = 50;

    initCNode(&task->ta_SoftTerm.si_Node);
    task->ta_SoftTerm.si_SIpl = -1;

    ++TaskCount;

    if (task->ta_Stack == MAP_FAILED) {
	taskDelete(task);
	return(NULL);
    }
#ifdef sun
    /*
     * XXX Solaris, associate guard page with stack
     */
#else
    mprotect(task->ta_Stack, TaskPgSize, 0);	/* Stack guard */
#endif

    /*
     * Kindof a hack to be portable.  Use the alternate signal stack to
     * start the thread.
     */
    bzero(&snew, sizeof(snew));
    snew.ss_sp = task->ta_Stack;
    snew.ss_size = task->ta_StackSize;
    snew.ss_flags = 0;
    if (sigaltstack(&snew, &sold) < 0)
	DBASSERT(0); 

    /*
     * Note: SA_NODEFER prevents the signal from being remasked by longjmp.
     */
    bzero(&sa, sizeof(sa));
    sa.sa_handler = taskStartupSig;
    sa.sa_flags = SA_ONSTACK | SA_RESETHAND | SA_NODEFER;

    /*
     * Make the task the current thread, then raise the signal to jump
     * to it (setting up the jumpbuf), then remove it from the list.
     * The task winds up in a 'waiting' state (not on any list).
     *
     * Don't bother adjusting SchedAggPri since it's net-0 (add to list,
     * remove from list).
     */
    insertNodeAfter(&TaskRun.ta_Node, &task->ta_Node);

    sigaction(SIGUSR1, &sa, &osa);
    if (sigsetjmp(StartupEnv, 0) == 0) {
	raise(SIGUSR1);
    }
    if (sigaltstack(&sold, NULL) < 0)
	DBASSERT(0); 
    sigaction(SIGUSR1, &osa, NULL);

    removeNode(&task->ta_Node);
    taskWakeup(task);

    return(task);
}

void 
taskDelete(Task *task)
{
    if (task == CURTASK) {
	moveNodeAfter(&TaskExit.ta_Node, &task->ta_Node);
	SchedAggPri -= task->ta_SchedPri;
	DBASSERT(SchedAggPri >= 0);
	task->ta_Flags |= TAF_EXITED | TAF_QUEUED;
	task->ta_Flags &= ~TAF_RUNNING;
	if (TaskCount == 1)	/* last task */
	    exit(0);
	taskSched();
	/* not reached */
    }

    DBASSERT(task->ta_SoftCount == 0);

    if (task->ta_Flags & (TAF_RUNNING|TAF_QUEUED)) {
	removeNode(&task->ta_Node);
	if (task->ta_Flags & TAF_RUNNING) {
	    SchedAggPri -= task->ta_SchedPri;
	    DBASSERT(SchedAggPri >= 0);
	}
    }

    --TaskCount;
    task->ta_Flags = TAF_EXITED;

    if (task->ta_Stack != MAP_FAILED) {
#ifdef sun
	free(task->ta_Stack);
#else
	munmap(task->ta_Stack, task->ta_StackSize);
#endif
	task->ta_Stack = MAP_FAILED;
    }
    zfree(task, sizeof(Task));
}

Task *
curTask(void)
{
    return(CURTASK);
}

void
taskSleep(int ms)
{
    SoftTimer timer;

    bzero(&timer, sizeof(timer));
    initTimer(&timer, ms, 0);
    startTimer(&timer);
    waitTimer(&timer);
}

void
taskWait(void)
{
    Task *task = CURTASK;

    /*
     * note: CURTASK becomes invalid when we remove our task from the 
     * run queue.
     */
    removeNode(&task->ta_Node);
    task->ta_SchedAccum = TaskQuantum;
    SchedAggPri -= task->ta_SchedPri;
    DBASSERT(SchedAggPri >= 0);
    task->ta_Flags &= ~TAF_RUNNING;
    if (sigsetjmp(task->ta_Env, 0) == 0)
	taskSched();

    /*
     * Check for exiting tasks
     */
    while ((task = getSucc(&TaskExit.ta_Node)) != &TaskExit)
	taskDelete(task);

    /*
     * Run any pending software interrupts
     */
    runSoftInts();
}

void
taskGiveup(void)
{
    Task *task = CURTASK;

    /*
     * If we do not have to poll for I/O yet and this is the only running
     * task, allow it to continue running.
     *
     * Note that we do NOT reset TaskQuantum here.  If we have a hog we
     * do not want to create unnecessary latency for other tasks.
     */
    if (IOQuantum > 0 && getPred(&TaskRun.ta_Node) == &task->ta_Node)
	return;

    /*
     * Return the (reduced) TaskQuantum to the task and move the task
     * to the end of the run queue and switch normally.   Adjust
     * ta_SchedAccum for a net-0 effect when the run queue comes around
     * to it again.  The run queue will add ta_SchedPri so we have
     * to subtract it here.  Otherwise we will not accumulate any
     * penalties for cpu-bound tasks.
     *
     * note: CURTASK becomes invalid when we remove our task from the 
     * run queue.  SchedAggPri does not change because the task stays
     * on the run queue.
     */
    removeNode(&task->ta_Node);
    insertNodeBefore(&TaskRun.ta_Node, &task->ta_Node);
    task->ta_SchedAccum = TaskQuantum - task->ta_SchedPri;

    /*
     * cpu hog
     */
    if (TaskQuantum <= 0 && task->ta_SchedPri > 10) {
	--task->ta_SchedPri;
	--SchedAggPri;
    }

    if (sigsetjmp(task->ta_Env, 0) == 0)
	taskSched();

    /*
     * On task resume, check for exiting tasks
     */
    while ((task = getSucc(&TaskExit.ta_Node)) != &TaskExit)
	taskDelete(task);

    /*
     * Run any pending software interrupts
     */
    runSoftInts();
}

void
taskWaitOnList(List *list)
{
    Task *task = CURTASK;

    /*
     * note: CURTASK becomes invalid when we remove our task from the 
     * run queue.
     */
    removeNode(&task->ta_Node);
    task->ta_SchedAccum = TaskQuantum;
    SchedAggPri -= task->ta_SchedPri;
    DBASSERT(SchedAggPri >= 0);
    addTail(list, &task->ta_Node);
    task->ta_Flags &= ~TAF_RUNNING;
    task->ta_Flags |= TAF_QUEUED;
    if (sigsetjmp(task->ta_Env, 0) == 0)
	taskSched();

    /*
     * Check for exiting tasks
     */
    while ((task = getSucc(&TaskExit.ta_Node)) != &TaskExit)
	taskDelete(task);

    /*
     * Run any pending software interrupts
     */
    runSoftInts();
}

void
taskSched(void)
{
    for (;;) {
	Task *task = CURTASK;	/* next runnable task */

	if (IOQuantum <= 0) {
	    struct timeval tv = { 0, 0 };

	    DisableQuantumInt = 1;
	    IOQuantum = 1;
	    DisableQuantumInt = 0;
	    _ioSelect(&tv);
	    testTimers(&tv);
	    continue;
	}

	/*
	 * If the run queue is not empty pull the next runnable task off
	 * the queue and give it some cpu.  If the task has really exhausted
	 * it's cpu then it may not get any until we loop.
	 */
	if (task != &TaskRun) {
	    if (task->ta_SchedAccum < TASK_QUANTUM_MAX)
		task->ta_SchedAccum += task->ta_SchedPri;
	    if (task->ta_SchedAccum <= 0) {
		removeNode(&task->ta_Node);
		insertNodeBefore(&TaskRun.ta_Node, &task->ta_Node);
		continue;
	    }
	    DisableQuantumInt = 1;
	    TaskQuantum = task->ta_SchedAccum;
	    DisableQuantumInt = 0;
	    siglongjmp(task->ta_Env, 1);
	    /* not reached */
	}
	{
	    struct timeval tv;
	    struct timeval *tvp;

	    tvp = testTimers(&tv);
	    if (tvp != NULL) {
		DisableQuantumInt = 1;
		IOQuantum = 1;
		DisableQuantumInt = 0;
		_ioSelect(tvp);
	    }
	}
    }
}

/*
 * taskWakeup() - wakeup a task
 *
 *	When waking a task up, try to give it cpu quickly by placing it
 *	at the head of the list.  However, if it's time accumulator is
 *	still negative prior to us adjusting it, add it to the end of
 *	the list.  This allows us to give priority to I/O bound tasks
 *	over cpu-bound tasks.
 */
void
taskWakeup(Task *task)
{
    DBASSERT(task != NULL);
    DBASSERT((task->ta_Flags & TAF_EXITED) == 0);
    if ((task->ta_Flags & (TAF_RUNNING|TAF_EXITED)) == 0) {
	if (task->ta_Flags & TAF_QUEUED) {
	    removeNode(&task->ta_Node);
	    task->ta_Flags &= ~TAF_QUEUED;
	}
	if (task->ta_SchedAccum <= 0) {
	    insertNodeAfter(&CURTASK->ta_Node, &task->ta_Node);
	} else {
	    insertNodeBefore(&TaskRun.ta_Node, &task->ta_Node);
	}
	/*
	 * recover scheduler priority if we slept
	 */
	if (task->ta_SchedPri < 25)
	    task->ta_SchedPri = 25;
	else if (task->ta_SchedPri < 50)
	    ++task->ta_SchedPri;
	if (task->ta_SchedAccum < TASK_QUANTUM_MAX)
	    task->ta_SchedAccum += task->ta_SchedPri;
	task->ta_Flags |= TAF_RUNNING;
	SchedAggPri += task->ta_SchedPri;
	DBASSERT(SchedAggPri >= 0);
    }
}

void
taskWakeupList(List *list)
{
    Task *task;

    while ((task = getHead(list)) != NULL) {
	DBASSERT(task->ta_Flags & TAF_QUEUED);
	taskWakeup(task);
    }
}

void
taskWakeupList1(List *list)
{
    Task *task;

    if ((task = getHead(list)) != NULL) {
	DBASSERT(task->ta_Flags & TAF_QUEUED);
	taskWakeup(task);
    }
}

int 
raiseSIpl(int sipl)
{
    Task *task = CURTASK;
    int oipl = task->ta_SIpl;

    if (sipl > oipl)
	task->ta_SIpl = sipl;
    return(oipl);
}

int 
setSIpl(int sipl)
{
    Task *task = CURTASK;
    int oipl = task->ta_SIpl;

    task->ta_SIpl = sipl;
    if (sipl < oipl)
	runSoftInts();
    return(oipl);
}

/*
 * taskStartupSig() - used to bootstrap a new thread
 *
 *	We use the alternate signal stack to setup a new thread on its
 *	stack, then generate a signal to start the thread running.
 */
static void
taskStartupSig(int sigNo)
{
    if (sigsetjmp(CURTASK->ta_Env, 0) == 1) {
	/* 
	 * Our frame is garbage since we are re-entering it
	 * in the middle after previously returning.
	 */
	CURTASK->ta_StartFunc(CURTASK->ta_StartData);
	taskDelete(CURTASK);
	/* not reached */
    }
}

/*
 * taskTimerSig() - scheduling quantum.
 *
 *	IOQuantum:	We want to poll our I/O system for work
 *
 *	TaskQuantum:	When this value goes negative the current
 *			task loses its quantum.  This value can become
 *			quite negative for a cpu-bound thread.
 */
static void
taskTimerSig(int sigNo)
{
    if (DisableQuantumInt == 0) {
	if (TaskQuantum >= TASK_QUANTUM_MIN)
	    TaskQuantum -= SchedAggPri;
	if (IOQuantum >= 0)
	    --IOQuantum;
    }
}

