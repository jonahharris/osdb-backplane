/*
 * HFLOCK.C	- Interface to Posix file locking
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/hflock.c,v 1.12 2002/08/20 22:05:55 dillon Exp $
 *
 *	Note restrictions: any file used for locking must be reopened after
 *	fork.
 */

#include "defs.h"

Export void hflock_ex(int fd, off_t offset);
Export void hflock_sh(int fd, off_t offset);
Export void hflock_un(int fd, off_t offset);
Export off_t hflock_alloc_ex(List *ltList, int fd, off_t begOff, off_t endOff, off_t bytes);
Export off_t hflock_find(List *ltList, int fd, off_t begOff, off_t endOff);
Export void hflock_downgrade(int fd, off_t off, off_t bytes);
Export void hflock_free(List *ltList, int fd, off_t pos, off_t bytes);

static int hflock_un_range(int fd, off_t offset, off_t bytes);

/*
 * hflock_ex() -	Exclusively lock four bytes at the specified offset
 */
void
hflock_ex(int fd, off_t offset)
{
    struct flock fl = { 0 };

    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = 4;

    if (fcntl(fd, F_SETLKW, &fl) < 0)
	DBASSERTF(0, ("Posix fcntl lock failed %s", strerror(errno)));
}

/*
 * hflock_sh() -	Shared lock of four bytes at the specified offset
 */
void
hflock_sh(int fd, off_t offset)
{
    struct flock fl = { 0 };

    fl.l_type = F_RDLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = 4;

    if (fcntl(fd, F_SETLKW, &fl) < 0)
	DBASSERTF(0, ("Posix fcntl lock failed"));
}

/*
 * hflock_un() -	Unlock a currently held shared or exclusive lock,
 *			4 bytes at the specified offset.
 */
void
hflock_un(int fd, off_t offset)
{
    struct flock fl = { 0 };

    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = 4;

    if (fcntl(fd, F_SETLKW, &fl) < 0)
	DBASSERTF(0, ("Posix fcntl lock failed"));
}

/*
 * HFLOCK_ALLOC() - allocate space in a file using POSIX locks
 *
 *	This function uses POSIX locks to reserve space in a file.  Space
 *	is reserved within the range provided using an exclusive lock.  -1
 *	is returned if no space could be reserved.
 *
 *	This function also tracks locks held by the local process and takes
 *	them into account.
 */
off_t
hflock_alloc_ex(List *ltList, int fd, off_t begOff, off_t endOff, off_t bytes)
{
    struct flock fl = { 0 };
    LockTrack *lt = getHead(ltList);

    fl.l_start = begOff;

    while (endOff - fl.l_start >= bytes) {
	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	fl.l_len = bytes;

	/*
	 * Internal collision?
	 */
	while (lt) {
	    if (lt->lt_Off + lt->lt_Bytes > fl.l_start)
		break;
	    lt = getListSucc(ltList, &lt->lt_Node);
	}
	if (lt && 
	    lt->lt_Off < fl.l_start + fl.l_len &&
	    lt->lt_Off + lt->lt_Bytes > fl.l_start
	) {
	    fl.l_start = lt->lt_Off + lt->lt_Bytes;
	    continue;
	}

	if (fcntl(fd, F_GETLK, &fl) < 0)
	    DBASSERTF(0, ("Posix fcntl lock failed"));

	/*
	 * If no lock blocks this one attempt to obtain an exclusive
	 * lock on the range.  If that fails then we raced another process
	 * (and lost), we loop and try again.
	 */
	if (fl.l_type == F_UNLCK) {
	    LockTrack *nlt;

	    fl.l_type = F_WRLCK;
	    if (fcntl(fd, F_SETLK, &fl) < 0)
		continue;
	    nlt = zalloc(sizeof(LockTrack));
	    nlt->lt_Off = fl.l_start;
	    nlt->lt_Bytes = fl.l_len;
	    if (lt)
		insertNodeBefore(&lt->lt_Node, &nlt->lt_Node);
	    else
		addTail(ltList, &nlt->lt_Node);
	    return(fl.l_start);
	}
	fl.l_start += fl.l_len;
    }
    return(-1);
}

/*
 * HFLOCK_FIND() - Locate the first locked area
 *
 *	Locate the begining of the first locked area within the specified
 *	range and return its base offset.  -1 is returned if no locked areas
 *	could be found.
 *
 *	This function uses the lock tracking done by hflock_alloc_ex() to
 *	locate and return internally held ranges, and POSIX functions to
 *	locate externally held ranges.
 */
off_t 
hflock_find(List *ltList, int fd, off_t begOff, off_t endOff)
{
    struct flock fl = { 0 };
    LockTrack *lt = getHead(ltList);

    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = begOff;
    fl.l_len = endOff - begOff;

    /*
     * Find the nearest internally held lock within the requested range.
     */
    while (lt) {
	if (lt->lt_Off + lt->lt_Bytes > fl.l_start)
	    break;
	lt = getListSucc(ltList, &lt->lt_Node);
    }
    if (lt->lt_Off >= endOff)
	lt = NULL;

    /*
     * Optimize the internal-lock-is-next-lock case
     */
    if (lt && begOff >= lt->lt_Off && begOff < lt->lt_Off + lt->lt_Bytes)
	return(begOff);

    if (fcntl(fd, F_GETLK, &fl) < 0)
	DBASSERTF(0, ("Posix fcntl lock failed"));

    /*
     * If no lock blocks this one (F_UNLOCK and lt NULL), the find failed
     * and we returnr -1.  Otherwise we take the nearest lock between 
     * lt and fl.l_start and return that.
     */
    if (fl.l_type == F_UNLCK) {
	if (lt == NULL)
	    return(-1);
    } else if (lt == NULL || fl.l_start < lt->lt_Off) {
	return(fl.l_start);
    }
    return(lt->lt_Off);
}

/*
 * HFLOCK_FREE() - free previously allocated space
 *
 *	Unlocks a range previously allocated by hflock_alloc_ex().  This
 *	function removes the associated tracking information and clears the
 *	POSIX lock.
 */
void
hflock_free(List *ltList, int fd, off_t pos, off_t bytes)
{
    LockTrack *lt;

    for (lt = getHead(ltList); lt; lt = getListSucc(ltList, &lt->lt_Node)) {
	if (lt->lt_Off == pos && lt->lt_Bytes == bytes) {
	    hflock_un_range(fd, pos, bytes);
	    removeNode(&lt->lt_Node);
	    zfree(lt, sizeof(LockTrack));
	    return;
	}
    }
    DBASSERT(0);
}

/*
 * HFLOCK_FREE() - free previously allocated space (static function)
 *
 *	Frees a previously held range allocated by hflock_alloc_ex().
 */

static int
hflock_un_range(int fd, off_t offset, off_t bytes)
{
    struct flock fl = { 0 };

    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = offset;
    fl.l_len = bytes;

    if (fcntl(fd, F_SETLKW, &fl) < 0)
	return(-1);
    return(0);
}

