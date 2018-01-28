/*
 * CURSOR/LEX.C -	Tokenizer, used by SQL parser
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT 
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/cursor/lex.c,v 1.11 2002/08/20 22:05:45 dillon Exp $
 */

#include "defs.h"

Export int SqlInit(token_t *t, const char *str, int len);
Export void SqlSetSubsection(token_t *t, const char *realBase, int realSize, int curLine);
Export void LexPrintError(token_t *t);

Prototype int SqlToken(token_t *t);
Prototype int SqlSkip(token_t *t, int type);
Prototype int SqlError(token_t *t, int tokErr);
Prototype const char *SqlTokenTypeDesc(int type);

const char *LexErrorLine(token_t *t, int *pline, int *plen, int *pi, const char **msg);

#define L_00	0x00	/* illegal lexical character */
#define L_WS	0x01	/* white space */
#define L_AL	0x02	/* alpha */
#define L_NU	0x04	/* numeric */
#define L_SP	0x08	/* single-character special */
#define L_MP	0x10	/* multi-character special */
#define L_QC	0x20	/* quote character */
#define L_XX	0x40	/* quote character */
#define L_AX	L_AL	/* special alpha */

static u_int8_t LexCharType[256] = {
    /* 00 */	L_XX,	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,
    /* 08 */	L_00,	L_WS,	L_WS,	L_WS,	L_WS,	L_00,	L_00,	L_00,
    /* 10 */	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,
    /* 18 */	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,	L_00,
    /* 20 */	L_WS,	L_MP,	L_QC,	L_XX,	L_AX,	L_MP,	L_MP,	L_QC,
    /* 28 */	L_SP,	L_SP,	L_MP,	L_MP,	L_SP,	L_MP,	L_SP,	L_MP,
    /* 30 */	L_NU,	L_NU,	L_NU,	L_NU,	L_NU,	L_NU,	L_NU,	L_NU,
    /* 38 */	L_NU,	L_NU,	L_SP,	L_SP,	L_MP,	L_MP,	L_MP,	L_MP,
    /* 40 */	L_MP,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,
    /* 48 */	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,
    /* 50 */	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,
    /* 58 */	L_AL,	L_AL,	L_AL,	L_SP,	L_SP,	L_SP,	L_MP,	L_AX,
    /* 60 */	L_00,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,
    /* 68 */	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,
    /* 70 */	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,	L_AL,
    /* 78 */	L_AL,	L_AL,	L_AL,	L_SP,	L_00,	L_SP,	L_MP,	L_00
};

#define SQL_HSIZE	16
#define SQL_HMASK	(SQL_HSIZE-1)

static int SqlDidInit;
static SqlKey SqlKeyAry[] = { INIT_KEYWORDS };
static SqlKey *SqlKeyHash[SQL_HSIZE];
static int CacheErrLine;
static const char *CacheErrPtr;
static const char *LexErrMsgs[] = { ERROR_STRINGS };

static
__inline int
sqlHash(u_int8_t c, int len)
{
    return(((int)c + len) & SQL_HMASK);
}

int
SqlInit(token_t *t, const char *str, int len)
{
    int i;

    t->t_Type = TOK_SOF;
    t->t_Data = str;
    t->t_Len = 0;
    t->t_FileBase = str;
    t->t_FileSize = len;
    t->t_FileLimit = len;
    t->t_Line = 1;
    CacheErrLine = 1;
    CacheErrPtr = str;

    if (SqlDidInit == 0) {
	SqlDidInit = 1;
	for (i = 0; i < arysize(SqlKeyAry); ++i) {
	    SqlKey *sk = &SqlKeyAry[i];
	    SqlKey **psk;

	    sk->sk_Len = strlen(sk->sk_Id);
	    psk = &SqlKeyHash[sqlHash((u_int8_t)*sk->sk_Id, sk->sk_Len)];
	    sk->sk_Next = *psk;
	    *psk = sk;
	}
    }
    return(SqlToken(t));
}

void
SqlSetSubsection(token_t *t, const char *realBase, int realSize, int curLine)
{
    t->t_FileLimit += t->t_Data - realBase;
    t->t_FileBase = realBase;
    t->t_FileSize = realSize;
    t->t_Line = curLine;
    CacheErrLine = curLine;
    CacheErrPtr = t->t_Data;
}

int
SqlToken(token_t *t)
{
    const char *base;
    const char *ptr;
    const char *fend;

    if (t->t_Type & (TOKF_ERROR|TOKF_EOF))
	return(t->t_Type);

    ptr = t->t_Data + t->t_Len;
    fend = t->t_FileBase + t->t_FileLimit;

    while (ptr < fend) {
	base = ptr;

	switch(LexCharType[(u_int8_t)*ptr]) {
	case L_WS:
	    if (*ptr == '\n')
		++t->t_Line;
	    ++ptr;
	    continue;
	case L_AL:
	    /*
	     * Alpha, convert to TOK_ID.  If the identifier matches some
	     * keyword then convert into TOK_<keyword>.  Such keyword tokens
	     * will have the TOKF_ID flag set (as does TOK_ID).
	     *
	     * id.id.id is treated as a single identifier.
	     */
	    for (
		++ptr;
		*ptr == '.' || (LexCharType[(u_int8_t)*ptr] & (L_AL|L_NU));
		++ptr
	    ) {
		;
	    }
	    t->t_Type = TOK_ID;
	    {
		SqlKey *sk;
		int hv = sqlHash(tolower((u_int8_t)*base), ptr - base);

		for (sk = SqlKeyHash[hv]; sk; sk = sk->sk_Next) {
		    if (sk->sk_Len == ptr - base &&
			strncmp(base, sk->sk_Id, sk->sk_Len) == 0
		    ) {
			t->t_Type = sk->sk_Tok;
			break;
		    }
		}
	    }
	    break;
	case L_NU:
	    t->t_Type = TOK_INT;
	    for (
		++ptr; 
		ptr < fend && (LexCharType[(u_int8_t)*ptr] & (L_AL|L_NU));
		++ptr
	    ) {
		;
	    }
	    if (*ptr == '.') {
		t->t_Type = TOK_REAL;
		for (
		    ++ptr; 
		    ptr < fend && (LexCharType[(u_int8_t)*ptr] & L_NU); 
		    ++ptr
		) {
		    ;
		}
		if (*ptr == 'e' || *ptr == 'E') {
		    if (ptr[1] == '+' || ptr[1] == '-')
			++ptr;
		    for (
			++ptr; 
			ptr < fend && (LexCharType[(u_int8_t)*ptr] & L_NU);
			++ptr
		    ) {
			;
		    }
		}
	    }
	    break;
	case L_MP:
	    /*
	     * Multi character special
	     *
	     *	< <= <> >= > =
	     */
	    if (*ptr == '<') {
		if (ptr[1] == '=') {
		    t->t_Type = TOK_LTEQ;
		    ptr += 2;
		    break;
		}
		if (ptr[1] == '>') {
		    t->t_Type = TOK_NOTEQ;
		    ptr += 2;
		    break;
		}
	    } else if (*ptr == '>') {
		if (ptr[1] == '=') {
		    t->t_Type = TOK_GTEQ;
		    ptr += 2;
		    break;
		}
	    }
	    /* fall through */
	case L_SP:
	    /*
	     * Single character special
	     */
	    if (ptr[0] == '/' && ptr + 1 < fend && ptr[1] == '*') {
		ptr += 2;
		while (ptr < fend) {
		    if (ptr[0] == '*' && ptr + 1 < fend && ptr[1] == '/') {
			ptr += 2;
			break;
		    }
		    if (*ptr == '\n')
			++t->t_Line;
		    ++ptr;
		}
		continue;
	    }
	    if (ptr[0] == '/' && ptr + 1 < fend && ptr[1] == '/') {
		while (ptr < fend && *ptr != '\n')
		    ++ptr;
		continue;
	    }
	    t->t_Type = (u_int8_t)*ptr;
	    ++ptr;
	    break;
	case L_QC:
	    /*
	     * Single or double quote
	     */
	    t->t_Type = (u_int8_t)*base;
	    for (++ptr; ptr < fend && *ptr != *base; ++ptr) {
		if (*ptr == '\\')
		    ++ptr;
		if (*ptr == 0 || *ptr == '\n') {
		    t->t_Type = TOK_ERR_UNTERMINATED_STRING;
		    break;
		}
	    }
	    if (ptr == fend)
		t->t_Type = TOK_ERR_UNTERMINATED_STRING;
	    else if (*ptr == *base)
		++ptr;
	    break;
	case L_XX:
	    switch(*ptr) {
	    case 0:
		/*
		 * Embedded terminator, usually allowed when parsing
		 * single-line buffers.
		 */
		t->t_Type = TOK_EOF1;
		break;
	    default:
		t->t_Type = *ptr;
		++ptr;
		break;
	    }
	    break;
	default:
	    t->t_Type = *ptr;
	    ++ptr;
	    break;
	}
	t->t_Data = base;
	t->t_Len = ptr - base;
	return(t->t_Type);
    }
    t->t_Data = ptr;
    t->t_Len = 0;
    t->t_Type = TOK_EOF2;
    return(t->t_Type);
}

int
SqlError(token_t *t, int tokErr)
{
    if ((t->t_Type & TOKF_ERROR) == 0)
	t->t_Type = tokErr;
    return(t->t_Type);
}

int
SqlSkip(token_t *t, int type)
{
    if (t->t_Type == type)
	return(SqlToken(t));
    return(SqlError(t, TOK_ERR_UNEXPECTED_TOKEN));
}

void
LexPrintError(token_t *t)
{
    const char *base;
    const char *msg;
    int lineNo, lineLen, ei;
    int errNo = (t->t_Type & TOKF_ERROR) ? (t->t_Type & 0xFFF) : -1;
    int i;

    base = LexErrorLine(t, &lineNo, &lineLen, &ei, &msg);
    fprintf(stderr, "Error line %d (%d) %s\n", lineNo, errNo, msg);
    for (i = 0; i < lineLen; ++i) {
	if (i == ei)
	    fprintf(stderr, "\033[7m");
	if (i == ei + t->t_Len)
	    fprintf(stderr, "\033[m");
	if (base[i] == '\t')
	    fputc(' ', stderr);
	else
	    fputc(base[i], stderr);
    }
    if (i == ei + t->t_Len)
	fprintf(stderr, "\033[m");
    fprintf(stderr, "\n");
}

const char *
LexErrorLine(token_t *t, int *pline, int *plen, int *pi, const char **msg)
{
    const char *ptr = CacheErrPtr;
    const char *pend;
    int eno = (t->t_Type & TOKF_ERROR) ? (t->t_Type & 0xFFF) : -1;

    /*
     * First locate the line containing the beginning of the token,
     * going backwards.
     */
    while (ptr > t->t_Data) {
	--ptr;
	if (*ptr == '\n')
	    --CacheErrLine;
    }

    /*
     * Locate the line containing the beginning of the token,
     * going forwards.
     */
    while (ptr < t->t_Data) {
	if (*ptr == '\n')
	    ++CacheErrLine;
	++ptr;
    }

    /*
     * Find the beginning of this line
     */
    while (ptr > t->t_FileBase && ptr[-1] != '\n')
	--ptr;

    /*
     * Locate the end of the line
     */
    CacheErrPtr = ptr;
    pend = ptr;
    while (pend < t->t_FileBase + t->t_FileSize && *pend != '\n')
	++pend;
    *pline = CacheErrLine;
    *plen = pend - ptr;
    *pi = t->t_Data - ptr;
    if (eno < 0 || eno > arysize(LexErrMsgs))
	*msg = "(Unknown Error)";
    else
	*msg = LexErrMsgs[eno];
    return(ptr);
}


const char *
SqlTokenTypeDesc(int type)
{
    int i;

    for (i = 0; i < arysize(SqlKeyAry); i++) {
	if (SqlKeyAry[i].sk_Tok == type)
	    return(SqlKeyAry[i].sk_Id);
    }

    return("UNKNOWN");
}

