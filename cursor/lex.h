/*
 * CURSOR/LEX.H	- Lexical tokens for SQL parser
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/cursor/lex.h,v 1.20 2002/08/20 22:05:45 dillon Exp $
 */

/*
 * token_t - lexical token
 */

typedef struct {
    int         t_Type;
    const char  *t_Data;
    int         t_Len;
    const char  *t_FileBase;
    int         t_FileLimit;
    int         t_FileSize;
    int		t_Line;
} token_t;

#define TOK_DOT		'.'
#define TOK_SEMI	';'
#define TOK_COMMA	','
#define TOK_OBRACE	'{'
#define TOK_CBRACE	'}'
#define TOK_OPAREN	'('
#define TOK_CPAREN	')'
#define TOK_OBRACKET	'['
#define TOK_CBRACKET	']'
#define TOK_STAR	'*'

#define TOK_EQ		'='
#define TOK_LT		'<'
#define TOK_GT		'>'
#define TOK_PLUS	'+'
#define TOK_MINUS	'-'

#define TOK_STRING	'\''
#define TOK_DQSTRING	'"'

#define TOK_EOF1	(TOKF_EOF+0x000) /* end of file (\0) */
#define TOK_EOF2	(TOKF_EOF+0x001) /* end of file (actual) */
#define TOK_ID		(TOKF_ID+0x000)

#define TOK_INSERT	(TOKF_ID+0x001)
#define TOK_DELETE	(TOKF_ID+0x002)
#define TOK_SELECT	(TOKF_ID+0x003)
#define TOK_UPDATE	(TOKF_ID+0x004)
#define TOK_CREATE	(TOKF_ID+0x005)
#define TOK_DROP	(TOKF_ID+0x006)
#define TOK_COUNT	(TOKF_ID+0x007)
#define TOK_CLONE	(TOKF_ID+0x008)

#define TOK_INTO	(TOKF_ID+0x100)
#define TOK_FROM	(TOKF_ID+0x101)
#define TOK_WHERE	(TOKF_ID+0x102)
#define TOK_SET		(TOKF_ID+0x103)
#define TOK_VALUES	(TOKF_ID+0x104)
#define TOK_USING	(TOKF_ID+0x105)
#define TOK_TABLE	(TOKF_ID+0x106)
#define TOK_SCHEMA	(TOKF_ID+0x107)
#define TOK_AND_CLAUSE	(TOKF_ID+0x108)
#define TOK_BEGIN	(TOKF_ID+0x109)
#define TOK_COMMIT	(TOKF_ID+0x10a)
#define TOK_ROLLBACK	(TOKF_ID+0x10b)
#define TOK_PRIMARY	(TOKF_ID+0x10c)
#define TOK_KEY		(TOKF_ID+0x10d)
#define TOK_NOT		(TOKF_ID+0x10e)
#define TOK_NULL	(TOKF_ID+0x10f)
#define TOK_ORDER	(TOKF_ID+0x110)
#define TOK_BY		(TOKF_ID+0x111)
#define TOK_DESC	(TOKF_ID+0x112)
#define TOK_LOAD	(TOKF_ID+0x113)
#define TOK_ALTER	(TOKF_ID+0x114)
#define TOK_COLUMN	(TOKF_ID+0x115)
#define TOK_ADD		(TOKF_ID+0x116)
#define TOK_DATA	(TOKF_ID+0x117)
#define TOK_TYPE	(TOKF_ID+0x118)
#define TOK_BEGINRO	(TOKF_ID+0x119)
#define TOK_LIMIT	(TOKF_ID+0x11a)
#define TOK_SYNC	(TOKF_ID+0x11b)
#define TOK_READONLY	(TOKF_ID+0x11c)
#define TOK_LIKE	(TOKF_ID+0x11d)
#define TOK_HISTORY	(TOKF_ID+0x11e)
#define TOK_STREAMING	(TOKF_ID+0x11f)
#define TOK_UNIQUE	(TOKF_ID+0x120)
#define TOK_SAME	(TOKF_ID+0x121)
#define TOK_return	(TOKF_ID+0x122)

#define TOK_SOF		(TOKF_MISC+0x000)
#define TOK_INT		(TOKF_MISC+0x001)
#define TOK_REAL	(TOKF_MISC+0x002)
#define TOK_LTEQ	(TOKF_MISC+0x003)
#define TOK_GTEQ	(TOKF_MISC+0x004)
#define TOK_NOTEQ	(TOKF_MISC+0x005)

#define TOKF_MISC	0x00001000
#define TOKF_ID		0x00002000
#define TOKF_EOF	0x40000000
#define TOKF_ERROR	0x80000000

typedef struct SqlKey {
    const char	*sk_Id;
    int		sk_Tok;
    int		sk_Len;
    struct SqlKey *sk_Next;
} SqlKey;

#define INIT_KEYWORDS	\
	{ "INSERT",	TOK_INSERT }, \
	{ "DELETE",	TOK_DELETE }, \
	{ "SELECT",	TOK_SELECT }, \
	{ "UPDATE",	TOK_UPDATE }, \
	{ "CREATE",	TOK_CREATE }, \
	{ "DROP",	TOK_DROP }, \
	{ "COUNT",	TOK_COUNT }, \
	{ "CLONE",	TOK_CLONE }, \
	{ "INTO",	TOK_INTO }, \
	{ "FROM",	TOK_FROM }, \
	{ "WHERE",	TOK_WHERE }, \
	{ "SET",	TOK_SET }, \
	{ "VALUES",	TOK_VALUES }, \
	{ "USING",	TOK_USING }, \
	{ "TABLE", 	TOK_TABLE }, \
	{ "SCHEMA", 	TOK_SCHEMA }, \
	{ "AND", 	TOK_AND_CLAUSE }, \
	{ "BEGIN", 	TOK_BEGIN }, \
	{ "BEGINRO", 	TOK_BEGINRO }, \
	{ "SYNC", 	TOK_SYNC }, \
	{ "READONLY", 	TOK_READONLY }, \
	{ "COMMIT", 	TOK_COMMIT }, \
	{ "ROLLBACK", 	TOK_ROLLBACK }, \
	{ "NULL",	TOK_NULL }, \
	{ "ORDER",	TOK_ORDER }, \
	{ "BY",		TOK_BY }, \
	{ "DESC",	TOK_DESC }, \
	{ "LIMIT",	TOK_LIMIT }, \
	{ "LOAD",	TOK_LOAD }, \
	{ "ALTER",	TOK_ALTER }, \
	{ "COLUMN",	TOK_COLUMN }, \
	{ "ADD",	TOK_ADD }, \
	{ "DATA",	TOK_DATA }, \
	{ "TYPE",	TOK_TYPE }, \
	{ "NOT", 	TOK_NOT }, \
	{ "PRIMARY",	TOK_PRIMARY }, \
	{ "KEY",	TOK_KEY }, \
	{ "LIKE",	TOK_LIKE }, \
	{ "SAME",	TOK_SAME }, \
	{ "HISTORY",	TOK_HISTORY }, \
	{ "STREAMING",	TOK_STREAMING }, \
	{ "UNIQUE", 	TOK_UNIQUE }, \
	{ "return", 	TOK_return }

#define TOK_ERR_PARSE_ERROR		(TOKF_ERROR|0x000)
#define TOK_ERR_UNRECOGNIZED_KEYWORD	(TOKF_ERROR|0x001)
#define TOK_ERR_UNTERMINATED_STRING	(TOKF_ERROR|0x002)
#define TOK_ERR_UNEXPECTED_TOKEN	(TOKF_ERROR|0x003)
#define TOK_ERR_TOO_MUCH_DATA		(TOKF_ERROR|0x004)
#define TOK_ERR_MUST_DECLARE_COLS	(TOKF_ERROR|0x005)
#define TOK_ERR_EXPECTED_COLUMN		(TOKF_ERROR|0x006)
#define TOK_ERR_EXPECTED_TABLE		(TOKF_ERROR|0x007)
#define TOK_ERR_EXPECTED_DATA		(TOKF_ERROR|0x008)
#define TOK_ERR_EXPECTED_ID		(TOKF_ERROR|0x009)
#define TOK_ERR_DUPLICATE_COLUMN	(TOKF_ERROR|0x00a)
#define TOK_ERR_MISSING_WHERE		(TOKF_ERROR|0x00b)
#define TOK_ERR_EXPECTED_OPERATOR	(TOKF_ERROR|0x00c)
#define TOK_ERR_CANNOT_HAVE_TWO_CONSTS	(TOKF_ERROR|0x00d)
#define TOK_ERR_TABLE_NOT_FOUND		(TOKF_ERROR|0x00e)
#define TOK_ERR_COLUMN_NOT_FOUND	(TOKF_ERROR|0x00f)
#define TOK_ERR_TABLE_REQUIRED		(TOKF_ERROR|0x010)
#define TOK_ERR_NOT_IN_TRANSACTION	(TOKF_ERROR|0x011)
#define TOK_ERR_WILDCARD_ILLEGAL	(TOKF_ERROR|0x012)
#define TOK_ERR_SYNTAX_ERROR		(TOKF_ERROR|0x013)
#define TOK_ERR_NO_DEFAULT_SCHEMA	(TOKF_ERROR|0x014)
#define TOK_ERR_SCHEMA_EXISTS		(TOKF_ERROR|0x015)
#define TOK_ERR_TABLE_EXISTS		(TOKF_ERROR|0x016)
#define TOK_ERR_COLUMN_EXISTS		(TOKF_ERROR|0x017)
#define TOK_ERR_UNRECOGNIZED_TYPE	(TOKF_ERROR|0x018)
#define TOK_ERR_CANT_PUSH		(TOKF_ERROR|0x019)
#define TOK_ERR_MACRO_SQL		(TOKF_ERROR|0x01a)
#define TOK_ERR_KEYNULL			(TOKF_ERROR|0x01b)
#define TOK_ERR_TOO_LITTLE_DATA		(TOKF_ERROR|0x01c)
#define TOK_ERR_RECORD_ALREADY		(TOKF_ERROR|0x01d)

#define TOKF_ERRMASK			0x0FFF

#define ERROR_STRINGS	\
	"General Parsing Error",	\
	"Unrecognized Command/Keyword",	\
	"Unterminated String",		\
	"Unexpected Token",		\
	"Too Much Data Specified",	\
	"Must Declare Columns for Data", \
	"Expected Column", \
	"Expected Table", \
	"Expected Data Item", \
	"Expected Id", \
	"Duplicate Column", \
	"Requires WHERE Clause", \
	"Expected Operator", \
	"Expression Cannot have Two Constants", \
	"Table not Found", \
	"Column not Found", \
	"Table Not Specified", \
	"You must be in a transaction to run SQL", \
	"Wildcard column not legal here", \
	"Syntax Error", \
	"No Default Schema, Schema must be Specified", \
	"Schema Exists", \
	"Table Exists", \
	"Column Exists", \
	"Unrecognized Type", \
	"Can't push transaction for macro SQL", \
	"Error executing macro SQL", \
	"KEY or NOT NULL field cannot be empty", \
	"Too Little Data Specified", \
	"Record Already Exists"

