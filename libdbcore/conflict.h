/*
 * LIBDBCORE/CONFLICT.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/conflict.h,v 1.6 2002/08/20 22:05:51 dillon Exp $
 */

struct CHead;
struct ConflictPos;

#define CH_MIN_NSLOT	32
#define CH_MAGIC	0x235FC32D
#define CH_NSLOT	1024
#define CH_VERSION	1

#define CH_ALIGN	(2 * 1024)
#define CH_MASK		(CH_ALIGN - 1)

#define CH_POS_LIMIT	2
#define CH_POS_MAPSIZE	(256*1024)
#define CH_POS_MAPMASK	(CH_POS_MAPSIZE - 1)

typedef struct Conflict {
    const struct CHead *co_Head;
    int		co_HeadMapSize;
    char	*co_FilePath;
    int		co_Refs;
    int		co_PosCount;
    int		co_Fd;
    pid_t	co_Pid;			/* represents my PID */
    List	co_LTList;		/* lock tracking list */
    List	co_PosList;		/* ConflictPos list */
} Conflict;

/*
 * ConflictPos - track conflict mapping
 *
 */
typedef struct ConflictPos {
    Node	cp_Node;
    dboff_t	cp_Off;
    const char	*cp_Base;
    int		cp_Bytes;
    int		cp_Refs;
} ConflictPos;

/*
 * CSlot - represents an instance in a phase-1 commit state.
 *
 *	The conflict file for a physical table contains an array of
 *	slots which database instances reserve during the phase-1 commit
 *	state.  In order to allow processes to be killed any instance
 *	with an active slot holds an exclusive lock on it and stores
 *	its PID in the slot.  To avoid stale pids (from killed processes)
 *	from intefering with a new client, said client will scan and
 *	remove any matching PIDs before it begins using the POSIX lock
 *	mechanism to reserve slots.
 *
 *	The sequence number is used to determine which records to scan
 *	during conflict resolution in commit phase-1.  Only records prior
 *	to our own sequence number need to be scanned.
 */

typedef struct CSlot {
    dboff_t	cs_Off;		/* offset of allocated block */
    dboff_t	cs_Size;	/* number of bytes of data */
    dboff_t	cs_Alloc;	/* size of the allocated block */
    dboff_t	cs_SeqNo;	/* sequence number (CSLOT_EMPTY if empty) */
    pid_t	cs_Pid;		/* owning pid */
} CSlot;

typedef struct CHead {
    int		ch_Magic;
    int		ch_Version;
    int		ch_Count;	/* number of slots */
    dboff_t	ch_SeqNo;	/* sequence number */
    CSlot	ch_Slots[1];	/* ch_Count slots */
} CHead;

