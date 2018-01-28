/*
 * LIBDBCORE/REPLICATE.C	- Replication Support
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/replicate.c,v 1.4 2002/08/20 22:05:52 dillon Exp $
 *
 *	Replication support involves extending the phase1/2 commit to cover
 *	multiple hosts and special-casing operations performed on the SYS
 *	database.
 *
 *	In general we have to pass along all queries made in a 
 *	modifying-transaction to the replication group.  Table and Schema
 *	references are passed by name, and column references are passed by
 *	column id (i.e. NOT by name).  Only those queries that fall within
 *	the replication group's schema/table specifications are passed.
 *
 *	Queries made on the SYS table are replicated as follows.  Note that
 *	high level SQL commands still go through the same 2-phase commit 
 *	process that low-level commands go through.
 *
 *	    CREATE/DROP SCHEMA	- replication by high level SQL command
 *				  only, and only if the schema itself is
 *				  being replicated (either specifically or
 *				  via a wildcard).
 *
 *	    CREATE/DROP TABLE	- replication by high level SQL command
 *				  only, and only if the table itself or
 *				  its governing schema is replicated.
 *
 *	    ALTER COLUMN	- since columns are really part of the table
 *				  and since column id's are synchronized
 *				  across replication hosts (unlike virtual
 *				  table id's), column manipulation is actually
 *				  replicated the same way data is replicated.
 *
 *	Essentially we must create a client instance on every replication
 *	host for our transaction.  We transmit our selections as they are
 *	issued and forward the phase1/2 commit.
 *
 *	The phase1 commit is forwarded only after it succeeds on the local
 *	host.  XXX deadlock situations between hosts, must order phase1's.
 *
 *	A quorum of hosts (including the local host) must respond for there
 *	to be success.
 */

#include "defs.h"

