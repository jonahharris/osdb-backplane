/*
 * LIBDBCORE/CONFLICT.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/conflict.c,v 1.17 2002/09/22 18:33:14 dillon Exp $
 */

#include "defs.h"
#include "conflict.h"

Prototype void CreateConflictArea(DataBase *db);
Prototype void AssertConflictAreaSize(DataBase *db);
Prototype void FreeConflictArea(DataBase *db);
Prototype void DestroyConflictArea(Table *tab);

Prototype int FindConflictSlot(struct Conflict *co, int slot);
Prototype const RecHead *FirstConflictRecord(struct Conflict *co, int slot, struct ConflictPos **ppos, dboff_t *pro);
Prototype const RecHead *NextConflictRecord(struct Conflict *co, int slot, struct ConflictPos **ppos, dboff_t *pro);
Prototype void ReleaseConflictRecord(struct Conflict *co, struct ConflictPos *pos);

static void OpenConflictArea(Table *tab);
static void CloseConflictArea(Table *tab);
static const void *mapConflictBlock(Conflict *co, ConflictPos **ppos, dboff_t ro, int bytes);
static void freeConflictPos(Conflict *co, ConflictPos *pos);

/*
 * CreateConflictArea() - Create a conflict block for the (modified) tables
 * 			  in a database during commit-1
 *
 *	XXX lock around tf_Append check?
 *
 *	Note that we no longer SyncTableAppend(), because that operation is
 *	now used only during commit-2 logging / file synchronization.  But
 *	we do synchronize the other way... that is, if the table is larger
 *	then our internal representation we fixup our internal
 *	representation.
 */
void
CreateConflictArea(DataBase *db)
{
    int i;

    for (i = 0; i < TAB_HSIZE; ++i) {
        Table *tab;
 
        for (tab = db->db_TabHash[i]; tab; tab = tab->ta_Next) {
	    Table *par = tab->ta_Parent;

	    DBASSERT(par != NULL);
	    if (tab->ta_Append < tab->ta_Meta->tf_Append)
		tab->ta_Append = tab->ta_Meta->tf_Append;
	    if (par->ta_Append < par->ta_Meta->tf_Append)
		par->ta_Append = par->ta_Meta->tf_Append;
#if 0
            /* removed */ SyncTableAppend(tab);
            /* removed */ SyncTableAppend(tab->ta_Parent);
#endif
	    OpenConflictArea(tab);
        }
    }
}

void
AssertConflictAreaSize(DataBase *db)
{
    int i;

    for (i = 0; i < TAB_HSIZE; ++i) {
        Table *tab;
 
        for (tab = db->db_TabHash[i]; tab; tab = tab->ta_Next) {
	    dboff_t bytes = tab->ta_Append - tab->ta_Meta->tf_DataOff;
	    dboff_t tbytes = 0;

	    bytes = (bytes + CH_MASK) & ~(dboff_t)CH_MASK;
	    if (tab->ta_TTsSlot >= 0)
		tbytes = tab->ta_Parent->ta_TTs->co_Head->ch_Slots[tab->ta_TTsSlot].cs_Alloc;
	    DBASSERT(tbytes == bytes);
        }
    }
}

void
FreeConflictArea(DataBase *db)
{
    int i;

    for (i = 0; i < TAB_HSIZE; ++i) {
        Table *tab;

        for (tab = db->db_TabHash[i]; tab; tab = tab->ta_Next) {
	    if (tab->ta_TTsSlot != -1)
		CloseConflictArea(tab);
	}
    }
}

/*
 *  OpenConflictArea() - setup tab->ta_TTs conflict area.
 */
void
OpenConflictArea(Table *tab)
{
    Conflict *co;
    struct stat st;
    Table *par = tab->ta_Parent;
    dboff_t bytes;
    dboff_t bytesAligned;

    /*
     * Reference the TTS file
     */

    if ((co = par->ta_TTs) == NULL) {
	CHead ch;
	int i;

	par->ta_TTs = co = zalloc(sizeof(Conflict));
	safe_asprintf(&co->co_FilePath, "%s/%s.tts", 
	    par->ta_Db->db_DirPath,
	    par->ta_Name
	);
	initList(&co->co_LTList);
	initList(&co->co_PosList);
	co->co_Fd = open(co->co_FilePath, O_RDWR|O_CREAT, 0660);
	co->co_Pid = getpid();
	co->co_Head = MAP_FAILED;
	DBASSERT(co->co_Fd >= 0);
	hflock_ex(co->co_Fd, 0);
	if (fstat(co->co_Fd, &st) < 0)
	    DBASSERT(0);

	/*
	 * Read tts header
	 */
	bzero(&ch, sizeof(ch));
	read(co->co_Fd, &ch, sizeof(ch));

	if (ch.ch_Magic != CH_MAGIC ||
	    ch.ch_Version != CH_VERSION ||
	    ch.ch_Count < CH_MIN_NSLOT ||
	    st.st_size < offsetof(CHead, ch_Slots[ch.ch_Count])
	) {
	    st.st_size = 0;
	}

	/*
	 * Initialize header if necessary
	 */
	lseek(co->co_Fd, 0L, 0);
	if (st.st_size < sizeof(CHead)) {
	    ch.ch_Magic = CH_MAGIC;
	    ch.ch_Version = CH_VERSION;
	    ch.ch_Count = CH_NSLOT;
	    ch.ch_SeqNo = 0;
	    st.st_size = offsetof(CHead, ch_Slots[CH_NSLOT]);
	    ftruncate(co->co_Fd, st.st_size);
	    write(co->co_Fd, &ch, sizeof(ch));
	}

	/*
	 * Map
	 */
	co->co_HeadMapSize = offsetof(CHead, ch_Slots[ch.ch_Count]);
	co->co_Head = mmap(
			NULL, 
			co->co_HeadMapSize,
			PROT_READ,
			MAP_SHARED,
			co->co_Fd,
			0
		    );
	DBASSERT(co->co_Head != MAP_FAILED);
	hflock_un(co->co_Fd, 0);

	/*
	 * Check for and destroy any stale entries that are using our PID.
	 * This allows us to avoid having to cleanup the file.
	 */
	for (i = 0; i < co->co_Head->ch_Count; ++i) {
	    if (co->co_Head->ch_Slots[i].cs_Pid == co->co_Pid) {
		CSlot cs;

		bzero(&cs, sizeof(cs));
		lseek(co->co_Fd, offsetof(CHead, ch_Slots[i]), 0);
		write(co->co_Fd, &cs, sizeof(cs));
	    }
	}
    }
    ++co->co_Refs;

    /*
     * Figure out how much space to reserve for the data, then allocate
     * a TTS slot and copy the data.
     */

    bytes = tab->ta_Append - tab->ta_Meta->tf_DataOff;
    bytesAligned = (bytes + CH_MASK) & ~(dboff_t)CH_MASK;

    if (bytes == 0) {
	tab->ta_TTsSlot = -2;
    } else {
	dboff_t off;
	CSlot cs = { 0 };

	off = hflock_alloc_ex(
		    &co->co_LTList,
		    co->co_Fd,
		    offsetof(CHead, ch_Slots[0]),
		    offsetof(CHead, ch_Slots[co->co_Head->ch_Count]),
		    sizeof(CSlot)
	);
	DBASSERT(off >= 0);	/* XXX pipeline phase-1 commits XXX */
	tab->ta_TTsSlot = (off - offsetof(CHead, ch_Slots[0])) / sizeof(CSlot);
	lseek(co->co_Fd, off, 0);		/* XXX excl lock win of opp*/
	write(co->co_Fd, &cs, sizeof(CSlot));
	cs.cs_Off = hflock_alloc_ex(
			&co->co_LTList,
			co->co_Fd,
			offsetof(CHead, ch_Slots[co->co_Head->ch_Count]),
			DBOFF_MAX,
			bytesAligned
		    );
	cs.cs_Size = bytes;
	cs.cs_Alloc = bytesAligned;
	cs.cs_SeqNo = co->co_Head->ch_SeqNo + 1;

#if 0
	printf("Allocate %d OFF %qx/%qx\n", tab->ta_TTsSlot, cs.cs_Off, cs.cs_Size);
#endif

	/*
	 * Copy the data
	 */
	lseek(co->co_Fd, cs.cs_Off, 0);
	{
	    RawData *rd;
	    TableI *ti;
	    int n = 0;
	    char buf[2048];

	    rd = AllocRawData(tab, NULL, 0);
	    ti = AllocPrivateTableI(rd);

	    GetLastTable(ti, NULL);
	    for (
		SelectBegTableRec(ti, 0);
		ti->ti_RanBeg.p_Ro >= 0;
		SelectNextTableRec(ti, 0)
	    ) {
		const RecHead *rh;
		int r;

		ReadDataRecord(rd, &ti->ti_RanBeg, 0);
		rh = rd->rd_Rh;
		DBASSERT(rh->rh_Size <= bytes);

		r = 0;
		while (r < rh->rh_Size) {
		    int u = rh->rh_Size - r;

		    if (u > sizeof(buf) - n)
			u = sizeof(buf) - n;
		    bcopy((const char *)rh + r, buf + n, u);
		    n += u;
		    r += u;
		    if (n == sizeof(buf)) {
			write(co->co_Fd, buf, n);
			n = 0;
		    }
		}
		bytes -= rh->rh_Size;
	    }
	    if (n != 0)
		write(co->co_Fd, buf, n);
	    LLFreeTableI(&ti);
	}
	cs.cs_Size -= bytes;	/* may be less without the block headers */

	/*
	 * Update the slot
	 */
	lseek(co->co_Fd, offsetof(CHead, ch_SeqNo), 0);	
	write(co->co_Fd, &cs.cs_SeqNo, sizeof(dboff_t));
	lseek(co->co_Fd, off, 0);		/* XXX excl lock win of opp*/
	write(co->co_Fd, &cs, sizeof(CSlot));
    }
}

void
CloseConflictArea(Table *tab)
{
    Table *par = tab->ta_Parent;
    Conflict *co = par->ta_TTs;

    /*
     * If not a degenerate case then deallocate the slot
     */
    if (tab->ta_TTsSlot >= 0) {
	CSlot cs = co->co_Head->ch_Slots[tab->ta_TTsSlot];
	CSlot zero = { 0 };

	lseek(co->co_Fd, offsetof(CHead, ch_Slots[tab->ta_TTsSlot]), 0);
	write(co->co_Fd, &zero, sizeof(zero));
	hflock_free(&co->co_LTList, co->co_Fd, offsetof(CHead, 
	    ch_Slots[tab->ta_TTsSlot]), sizeof(CSlot));
	if (cs.cs_Alloc)
	    hflock_free(&co->co_LTList, co->co_Fd, cs.cs_Off, cs.cs_Alloc);
#if 0
	printf("Free %d OFF %qx/%qx\n", tab->ta_TTsSlot, cs.cs_Off, cs.cs_Size);
#endif
    }
    tab->ta_TTsSlot = -1;

    /*
     * Release the reference to the Conflict structure, which is stored
     * relative to the parent table (so all the clients can share it).
     */
    DBASSERT(co != NULL);
    DBASSERT(co->co_Refs > 0);
    --co->co_Refs;
}

/*
 * DestroyConflictArea()- called when associated table is being destroyed or
 *			  flushed.
 *
 *	This routine is only called when the associated table (aka the
 *	one representing the physical table file) is being destroyed or
 *	flushed.
 */

void
DestroyConflictArea(Table *tab)
{
    Conflict *co;

    if ((co = tab->ta_TTs) != NULL) {
	ConflictPos *pos;

	DBASSERT(co->co_Refs == 0);
	while ((pos = getHead(&co->co_PosList)) != NULL) {
	    DBASSERT(pos->cp_Refs == 0);
	    freeConflictPos(co, pos);
	}
	DBASSERT(co->co_PosCount == 0);
	close(co->co_Fd);
	co->co_Fd = -1;
	safe_free(&co->co_FilePath);
	DBASSERT(co->co_Head != MAP_FAILED);
	munmap((void *)co->co_Head, co->co_HeadMapSize);
	co->co_Head = MAP_FAILED;
	zfree(co, sizeof(Conflict));
	tab->ta_TTs = NULL;
    }
}

/*
 * FindConflictSlot() - Locate next valid conflict slot
 */

int
FindConflictSlot(Conflict *co, int slot)
{
    off_t off;

    off = hflock_find(
		&co->co_LTList,
		co->co_Fd,
		offsetof(CHead, ch_Slots[slot]),
		offsetof(CHead, ch_Slots[co->co_Head->ch_Count])
    );
    if (off >= 0)
	return((int)(off - offsetof(CHead, ch_Slots[0])) / sizeof(CSlot));
    return(-1);
}

const RecHead *
FirstConflictRecord(Conflict *co, int slot, ConflictPos **ppos, dboff_t *pro)
{
    const CSlot *cs = &co->co_Head->ch_Slots[slot];
    const RecHead *rh;

    if (cs->cs_Size) {
	*pro = cs->cs_Off;
	*ppos = NULL;
	rh = mapConflictBlock(co, ppos, *pro, sizeof(RecHead));
	rh = mapConflictBlock(co, ppos, *pro, rh->rh_Size);
    } else {
	*pro = 0;
	*ppos = NULL;
	rh = NULL;
    }
    return(rh);
}

const RecHead *
NextConflictRecord(Conflict *co, int slot, ConflictPos **ppos, dboff_t *pro)
{
    const CSlot *cs = &co->co_Head->ch_Slots[slot];
    const RecHead *rh;

    rh = mapConflictBlock(co, ppos, *pro, sizeof(RecHead));
    if (*pro + rh->rh_Size != cs->cs_Off + cs->cs_Size) {
	DBASSERT(*pro + rh->rh_Size < cs->cs_Off + cs->cs_Size);
	*pro += rh->rh_Size;
	rh = mapConflictBlock(co, ppos, *pro, sizeof(RecHead));
	rh = mapConflictBlock(co, ppos, *pro, rh->rh_Size);
    } else {
	rh = NULL;
    }
    return(rh);
}

void
ReleaseConflictRecord(Conflict *co, ConflictPos *pos)
{
    if (pos) {
	DBASSERT(pos->cp_Refs > 0);
	if (--pos->cp_Refs == 0) {
	    if (co->co_PosCount > CH_POS_LIMIT) {
		freeConflictPos(co, pos);
	    }
	}
    }
}

const void *
mapConflictBlock(Conflict *co, ConflictPos **ppos, dboff_t ro, int bytes)
{
    ConflictPos *pos;

    if ((pos = *ppos) != NULL) {
	if (ro >= pos->cp_Off && ro + bytes <= pos->cp_Off + pos->cp_Bytes)
	    return(pos->cp_Base + (ro - pos->cp_Off));
	ReleaseConflictRecord(co, pos);
	*ppos = NULL;
    }
    for (
	pos = getHead(&co->co_PosList);
	pos;
	pos = getListSucc(&co->co_PosList, &pos->cp_Node)
    ) {
	if (ro >= pos->cp_Off && ro + bytes <= pos->cp_Off + pos->cp_Bytes)
	    break;
    }
    if (pos == NULL) {
	pos = zalloc(sizeof(ConflictPos));
	pos->cp_Off = (ro & ~(dboff_t)CH_POS_MAPMASK);
	pos->cp_Bytes = ((int)(ro - pos->cp_Off) + bytes + CH_POS_MAPMASK) &
			    ~(CH_POS_MAPMASK);
	pos->cp_Base = mmap(NULL, pos->cp_Bytes, 
			    PROT_READ, MAP_SHARED, co->co_Fd, pos->cp_Off);
	DBASSERT(pos->cp_Base != MAP_FAILED);
	addHead(&co->co_PosList, &pos->cp_Node);
	++co->co_PosCount;
    }
    ++pos->cp_Refs;
    *ppos = pos;
    return(pos->cp_Base + (ro - pos->cp_Off));
}

void
freeConflictPos(Conflict *co, ConflictPos *pos)
{
    DBASSERT(pos->cp_Refs == 0);
    removeNode(&pos->cp_Node);
    --co->co_PosCount;
    munmap((void *)pos->cp_Base, pos->cp_Bytes);
    pos->cp_Base = MAP_FAILED;
    zfree(pos, sizeof(ConflictPos));
}

