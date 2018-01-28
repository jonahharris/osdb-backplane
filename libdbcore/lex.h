/*
 * LIBDBCORE/LEX.H	- Lexical tokens for SQL parser
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libdbcore/lex.h,v 1.20 2002/08/20 22:05:51 dillon Exp $
 */

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
#define TOK_LIKE	(TOKF_ID+0x11b)
#define TOK_HISTORY	(TOKF_ID+0x11c)
#define TOK_STREAMING	(TOKF_ID+0x11d)
#define TOK_READONLY	(TOKF_ID+0x11e)
#define TOK_SYNC	(TOKF_ID+0x11f)
#define TOK_UNIQUE	(TOKF_ID+0x120)
#define TOK_SAME	(TOKF_ID+0x121)
#define TOK_DEFAULT	(TOKF_ID+0x122)

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
	{ "insert",	TOK_INSERT }, \
	{ "delete",	TOK_DELETE }, \
	{ "select",	TOK_SELECT }, \
	{ "update",	TOK_UPDATE }, \
	{ "create",	TOK_CREATE }, \
	{ "drop",	TOK_DROP }, \
	{ "count",	TOK_COUNT }, \
	{ "clone",	TOK_CLONE }, \
	{ "into",	TOK_INTO }, \
	{ "from",	TOK_FROM }, \
	{ "where",	TOK_WHERE }, \
	{ "set",	TOK_SET }, \
	{ "values",	TOK_VALUES }, \
	{ "using",	TOK_USING }, \
	{ "table", 	TOK_TABLE }, \
	{ "schema", 	TOK_SCHEMA }, \
	{ "and", 	TOK_AND_CLAUSE }, \
	{ "begin", 	TOK_BEGIN }, \
	{ "beginro", 	TOK_BEGINRO }, \
	{ "streaming",	TOK_STREAMING }, \
	{ "commit", 	TOK_COMMIT }, \
	{ "rollback", 	TOK_ROLLBACK }, \
	{ "primary", 	TOK_PRIMARY }, \
	{ "key", 	TOK_KEY }, \
	{ "not", 	TOK_NOT }, \
	{ "null", 	TOK_NULL }, \
	{ "order",	TOK_ORDER }, \
	{ "by",		TOK_BY }, \
	{ "desc",	TOK_DESC }, \
	{ "limit",	TOK_LIMIT }, \
	{ "load",	TOK_LOAD }, \
	{ "alter",	TOK_ALTER }, \
	{ "column",	TOK_COLUMN }, \
	{ "add",	TOK_ADD }, \
	{ "data",	TOK_DATA }, \
	{ "type",	TOK_TYPE }, \
	{ "like",	TOK_LIKE }, \
	{ "history",	TOK_HISTORY }, \
	{ "sync",	TOK_SYNC }, \
	{ "readonly",	TOK_READONLY }, \
	{ "unique",	TOK_UNIQUE }, \
	{ "same",	TOK_SAME }, \
	{ "default",	TOK_DEFAULT }

#define TOKF_ERRMASK			0x0FFF
#define DBTOKTOERR(dberr)	(((-dberr) & TOKF_ERRMASK) | TOKF_ERROR)

