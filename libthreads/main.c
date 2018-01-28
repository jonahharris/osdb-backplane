/*
 * MAIN.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 *	Provide a main() entry point which initializes the threading system
 *	and calls the user entry point task_main().
 *
 *	Note that the threading system only exit()s after the last thread
 *	has been deleted.
 *
 * $Backplane: rdbms/libthreads/main.c,v 1.5 2002/08/20 22:05:57 dillon Exp $
 */

#include "defs.h"

struct main_args {
    int ac;
    char **av;
};

void __task_main(struct main_args *ma);
void task_main(int ac, char **av);

int
main(int ac, char **av)
{
    struct main_args ma;

    ma.ac = ac;
    ma.av = av;
    taskInitThreads();
    taskWakeup(taskCreate(__task_main, &ma));
    taskSched();
    /* not reached */
    return(0);
}

void
__task_main(struct main_args *ma)
{
    task_main(ma->ac, ma->av);
}

