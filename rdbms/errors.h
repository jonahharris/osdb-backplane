/*
 * RDBMS/ERRORS.H
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/rdbms/errors.h,v 1.16 2002/08/20 22:05:58 dillon Exp $
 */

#define DBERR_GENERAL		-1	/* catch-all */
#define DBERR_CANTMAKEDIR	-2
#define DBERR_CANT_OPEN		-3
#define DBERR_CANT_CREATE	-4
#define DBERR_NO_MEM		-5
#define DBERR_MEM_CORRUPT	-6
#define DBERR_CANT_CONNECT	-7	/* cannot connect to replicator */
#define DBERR_LIMIT_ABORT	-8	/* query limit hit (internal only) */

#define DBERR_TABLE_MAGIC	-16
#define DBERR_TABLE_VERSION	-17
#define DBERR_TABLE_TYPE	-18
#define DBERR_TABLE_TRUNC	-19
#define DBERR_TABLE_CORRUPT	-20
#define DBERR_TABLE_MMAP	-21
#define DBERR_TABLE_READ	-22
#define DBERR_TABLE_WRITE	-23

#define DBERR_NEED_TABLE	-24	/* table specification missing */
#define DBERR_ONE_TABLE_ONLY	-25	/* only one table spec allowed */
#define DBERR_UNBOUND_RANGE	-26	/* unbounded range */
#define DBERR_GROUPID_MISMATCH	-27	/* database groupid is wrong */

#define DBERR_PARSE_ERROR		-30
#define DBERR_UNRECOGNIZED_KEYWORD	-31
#define DBERR_UNTERMINATED_STRING	-32
#define DBERR_UNEXPECTED_TOKEN		-33
#define DBERR_TOO_MUCH_DATA		-34
#define DBERR_MUST_DECLARE_COLS		-35
#define DBERR_EXPECTED_COLUMN		-36
#define DBERR_EXPECTED_TABLE		-37
#define DBERR_EXPECTED_DATA		-38
#define DBERR_EXPECTED_ID		-39
#define DBERR_DUPLICATE_COLUMN		-40
#define DBERR_MISSING_WHERE		-41
#define DBERR_EXPECTED_OPERATOR		-42
#define DBERR_CANNOT_HAVE_TWO_CONSTS	-43
#define DBERR_TABLE_NOT_FOUND		-44
#define DBERR_COLUMN_NOT_FOUND		-45
#define DBERR_TABLE_REQUIRED		-46
#define DBERR_NOT_IN_TRANSACTION	-47
#define DBERR_WILDCARD_ILLEGAL		-48
#define DBERR_SYNTAX_ERROR		-49
#define DBERR_NO_DEFAULT_SCHEMA		-50
#define DBERR_SCHEMA_EXISTS		-51
#define DBERR_TABLE_EXISTS		-52
#define DBERR_COLUMN_EXISTS		-53
#define DBERR_UNRECOGNIZED_TYPE		-54
#define DBERR_CANT_PUSH			-55
#define DBERR_MACRO_SQL			-56
#define DBERR_KEYNULL			-57
#define DBERR_TOO_LITTLE_DATA		-58
#define DBERR_RECORD_ALREADY		-59
#define DBERR_UNRECOGNIZED_ATTR		-60
#define DBERR_SCHEMA_NOT_FOUND		-61
#define DBERR_CANT_REMOVE_SYS_SCHEMA	-62
#define DBERR_CANT_RM_SCH_WITH_TABLES	-63
#define DBERR_FEATURE_NOT_SUPPORTED	-64
#define DBERR_COMMIT2_WITHOUT_COMMIT1	-65
#define DBERR_COMMIT1_CONFLICT		-66
#define DBERR_COMMIT2_DELAY_ABORT	-67	/* delayed c2 abrt if quorum */ 
#define DBERR_ABORT_OUTSIDE_TRANS	-68	/* abort outside trans */
#define DBERR_SELECT_BREAK		-69	/* interrupted query */
#define DBERR_LOST_LINK			-70	/* link lost during strm qry */
#define DBERR_NOT_BOTH_UNIQUE_PRIMARY	-71	/* can't have both */
#define DBERR_DUPLICATE_DEFAULT		-72	/* multiple defaults for col */

#define DBERR_REP_NOT_IN_TREE		-128	/* db not in spanning tree */
#define DBERR_REP_UNASSOCIATED_COPIES	-129	/* multiple unassoc db's */
#define DBERR_REP_BAD_CREATE_TS		-130	/* db in tree missing ts */
#define DBERR_REP_ALREADY_EXISTS_LOCAL	-131	/* local copy already there */
#define DBERR_REP_CORRUPT_REPGROUP	-132	/* sys.repgroup is corrupt */
#define DBERR_REP_FAILED_NO_QUORUM	-133	/* need quorum for operation */
#define DBERR_REP_INSTANCE_OPEN_FAILED	-134	/* failed to open db instance */
#define DBERR_REP_DATABASE_OPEN_FAILED	-135	/* failed to open db */

#define ERROR_STRINGS	\
	/* 00 */	"",				\
	/* 01 */	"General Database Error",	\
	/* 02 */	"Can't Make Directory",		\
	/* 03 */	"Can't Open Table/DB",		\
	/* 04 */	"Can't Create Table/DB",	\
	/* 05 */	"Ran out of Memory",		\
	/* 06 */	"Memory Corrupted",		\
	/* 07 */	"Failed to Connect to Replicator",\
	/* 08 */	"Limit Abort (internal only)",	\
	/* 09 */	"Error -9",			\
	/* 10 */	"Error -10",			\
	/* 11 */	"Error -11",			\
	/* 12 */	"Error -12",			\
	/* 13 */	"Error -13",			\
	/* 14 */	"Error -14",			\
	/* 15 */	"Error -15",			\
	/* 16 */	"Bad Physical Table Magic",		\
	/* 17 */	"Bad Physical Table Version",		\
	/* 18 */	"Bad Physical Table Type",		\
	/* 19 */	"Physical Table Was Truncated",		\
	/* 20 */	"Physical Table Corrupted",		\
	/* 21 */	"Physical Table MMap Failed",		\
	/* 22 */	"Physical Table Read I/O Failed",	\
	/* 23 */	"Physical Table Write I/O Failed",	\
	/* 24 */	"Table Not Specified",			\
	/* 25 */	"Only One Table May be Specified",	\
	/* 26 */	"Unbounded Range",			\
	/* 27 */	"Database GroupId Mismatch",		\
	/* 28 */	"Error -28",				\
	/* 29 */	"Error -29",				\
	/* 30 */	"General Parsing Error",		\
	/* 31 */	"Unrecognized Command/Keyword",		\
	/* 32 */	"Unterminated String",			\
	/* 33 */	"Unexpected Token",			\
	/* 34 */	"Too Much Data Specified",		\
	/* 35 */	"Must Declare Columns for Data",	\
	/* 36 */	"Expected Column", 			\
	/* 37 */	"Expected Table", 			\
	/* 38 */	"Expected Data Item", 			\
	/* 39 */	"Expected Id", 				\
	/* 40 */	"Duplicate Column", 			\
	/* 41 */	"Requires WHERE Clause",	 	\
	/* 42 */	"Expected Operator", 			\
	/* 43 */	"Expression Cannot have Two Constants", \
	/* 44 */	"Table not Found",			\
	/* 45 */	"Column not Found",			\
	/* 46 */	"Table Not Specified",			\
	/* 47 */	"You must be in a transaction to run SQL", \
	/* 48 */	"Wildcard column not legal here",	\
	/* 49 */	"Syntax Error",				\
	/* 50 */	"No Default Schema, Schema must be Specified", \
	/* 51 */	"Schema Exists",			\
	/* 52 */	"Table Exists",				\
	/* 53 */	"Column Exists",			\
	/* 54 */	"Unrecognized Type",			\
	/* 55 */	"Can't push transaction for macro SQL",	\
	/* 56 */	"Error executing macro SQL",		\
	/* 57 */	"KEY or NOT NULL field cannot be empty",\
	/* 58 */	"Too Little Data Specified",		\
	/* 59 */	"Record Already Exists",		\
	/* 60 */	"Unrecognized Attribute",		\
	/* 61 */	"Schema Not Found",			\
	/* 62 */	"Cannot Remove Reserved Schema",	\
	/* 63 */	"Cannot Remove Schema With Live Tables",\
	/* 64 */	"SQL Feature not supported",		\
	/* 65 */	"Commit2 without Commit1",		\
	/* 66 */	"Commit1 Conflict",			\
	/* 67 */	"Delayed commit2 aborted after quorum reached", \
	/* 68 */	"Abort without a transaction to abort", \
	/* 69 */	"Query Interrupted", \
	/* 70 */	"Link lost during streaming query", \
	/* 71 */	"Cannot have both UNIQUE and PRIMARY KEY for field", \
	/* 72 */	"Duplicate default clause", \
	/* 73 */	UnknownError, \
	/* 74 */	UnknownError, \
	/* 75 */	UnknownError, \
	/* 76 */	UnknownError, \
	/* 77 */	UnknownError, \
	/* 78 */	UnknownError, \
	/* 79 */	UnknownError, \
	/* 80 */	UnknownError, \
	/* 81 */	UnknownError, \
	/* 82 */	UnknownError, \
	/* 83 */	UnknownError, \
	/* 84 */	UnknownError, \
	/* 85 */	UnknownError, \
	/* 86 */	UnknownError, \
	/* 87 */	UnknownError, \
	/* 88 */	UnknownError, \
	/* 89 */	UnknownError, \
	/* 90 */	UnknownError, \
	/* 91 */	UnknownError, \
	/* 92 */	UnknownError, \
	/* 93 */	UnknownError, \
	/* 94 */	UnknownError, \
	/* 95 */	UnknownError, \
	/* 96 */	UnknownError, \
	/* 97 */	UnknownError, \
	/* 98 */	UnknownError, \
	/* 99 */	UnknownError, \
	/* 100 */	UnknownError, \
	/* 101 */	UnknownError, \
	/* 102 */	UnknownError, \
	/* 103 */	UnknownError, \
	/* 104 */	UnknownError, \
	/* 105 */	UnknownError, \
	/* 106 */	UnknownError, \
	/* 107 */	UnknownError, \
	/* 108 */	UnknownError, \
	/* 109 */	UnknownError, \
	/* 110 */	UnknownError, \
	/* 111 */	UnknownError, \
	/* 112 */	UnknownError, \
	/* 113 */	UnknownError, \
	/* 114 */	UnknownError, \
	/* 115 */	UnknownError, \
	/* 116 */	UnknownError, \
	/* 117 */	UnknownError, \
	/* 118 */	UnknownError, \
	/* 119 */	UnknownError, \
	/* 120 */	UnknownError, \
	/* 121 */	UnknownError, \
	/* 122 */	UnknownError, \
	/* 123 */	UnknownError, \
	/* 124 */	UnknownError, \
	/* 125 */	UnknownError, \
	/* 126 */	UnknownError, \
	/* 127 */	UnknownError, \
	/* 128 */	"Database not in spanning tree", \
	/* 129 */	"Duplicate non-integrated databases in spanning tree", \
	/* 130 */	"Bad database create timestamp", \
	/* 131 */	"Database already exists locally", \
	/* 132 */	"sys.repgroup table is corrupt", \
	/* 133 */	"Quorum of PEERs required for operation", \
	/* 134 */	"Replicator: Unable to open database instance", \
	/* 135 */	"Replicator: Unable to open database", \
	/* 136 */	UnknownError, \
	/* 137 */	UnknownError, \
	/* 138 */	UnknownError, \
	/* 139 */	UnknownError, \
	/* 140 */	UnknownError, \
	/* 141 */	UnknownError, \
	/* 142 */	UnknownError, \
	/* 143 */	UnknownError, \
	/* 144 */	UnknownError, \
	/* 145 */	UnknownError, \
	/* 146 */	UnknownError, \
	/* 147 */	UnknownError, \
	/* 148 */	UnknownError, \
	/* 149 */	UnknownError

