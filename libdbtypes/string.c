/*
 * LIBDBTYPES/STRING.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Implements string operators, validation, encoding, and decoding
 *	functions.
 *
 * $Backplane: rdbms/libdbtypes/string.c,v 1.3 2002/08/20 22:05:53 dillon Exp $
 */

#include "defs.h"

static int OpLikeStrMatch(const ColData *d1, const ColData *d2);
static int OpLikeNoStrMatch(const ColData *d1, const ColData *d2);

static int OpSameStrMatch(const ColData *d1, const ColData *d2);
static int OpSameNoStrMatch(const ColData *d1, const ColData *d2);

static int OpLtStrMatch(const ColData *d1, const ColData *d2);
static int OpGtStrMatch(const ColData *d1, const ColData *d2);
static int OpLtEqStrMatch(const ColData *d1, const ColData *d2);
static int OpGtEqStrMatch(const ColData *d1, const ColData *d2);

Prototype dataop_func_t DataTypeStringFuncAry[];

dataop_func_t DataTypeStringFuncAry[] = {
	OpUnknown,		/* DATATYPE_UNKNOWN	*/
	OpLikeStrMatch,		/* DATATYPE_LIKE	*/
	OpLikeNoStrMatch,	/* DATATYPE_RLIKE	*/
	OpSameStrMatch,		/* DATATYPE_SAME	*/
	OpSameNoStrMatch,	/* DATATYPE_RSAME	*/
	OpExactMatch,		/* DATATYPE_EQEQ	*/
	OpExactNoMatch,		/* DATATYPE_NOTEQ	*/
	OpLtStrMatch,		/* DATATYPE_LT		*/
	OpLtEqStrMatch,		/* DATATYPE_LTEQ	*/
	OpGtStrMatch,		/* DATATYPE_GT 		*/
	OpGtEqStrMatch		/* DATATYPE_GTEQ	*/
};

static int 
OpLtStrMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	if (d1->cd_Data[i] == d2->cd_Data[i])
	    continue;
	if (d1->cd_Data[i] < d2->cd_Data[i])
	    return(1);		/* TRUE region #1 (d1 smaller) */
	return(-1);		/* FALSE region #1 (d1 larger) */
    }
    if (s == d1->cd_Bytes) {
	if (s == d2->cd_Bytes)
	    return(-1);		/* FALSE region #1 (exact match) */
	return(1);		/* TRUE region #1 (d1 smaller) */
    }
    return(-1);			/* FALSE region #1 (d1 larger) */
}

static int
OpGtStrMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	if (d1->cd_Data[i] == d2->cd_Data[i])
	    continue;
	if (d1->cd_Data[i] > d2->cd_Data[i])
	    return(1);		/* TRUE region #1 (d1 larger) */
	return(-1);		/* FALSE region #1 (d1 smaller) */
    }
    if (s == d1->cd_Bytes) {
	if (s == d2->cd_Bytes)
	    return(-1);		/* FALSE region #1 (exact match) */
	return(-1);		/* FALSE region #1 (d1 smaller) */
    }
    return(1);			/* TRUE region #1 (d1 larger) */
}

static int
OpLtEqStrMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	if (d1->cd_Data[i] == d2->cd_Data[i])
	    continue;
	if (d1->cd_Data[i] < d2->cd_Data[i])
	    return(1);		/* TRUE region #1 (d1 smaller) */
	return(-1);		/* FALSE region #1 (d1 larger) */
    }
    if (s == d1->cd_Bytes) {
	if (s == d2->cd_Bytes)
	    return(1);		/* TRUE region #1 (exact match) */
	return(1);		/* TRUE region #1 (d1 smaller) */
    }
    return(-1);			/* FALSE region #1 (d1 larger) */
}

static int
OpGtEqStrMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	if (d1->cd_Data[i] == d2->cd_Data[i])
	    continue;
	if (d1->cd_Data[i] > d2->cd_Data[i])
	    return(1);		/* TRUE region #1 (d1 larger) */
	return(-1);		/* FALSE region #1 (d1 smaller) */
    }
    if (s == d1->cd_Bytes) {
	if (s == d2->cd_Bytes)
	    return(1);		/* TRUE region #1 (exact match) */
	return(-1);		/* FALSE region #1 (d1 smaller) */
    }
    return(1);			/* TRUE region #1 (d1 larger) */
}

static int
OpLikeNoStrMatch(const ColData *d1, const ColData *d2)
{
    int i;

    if (d1->cd_Bytes > d2->cd_Bytes)	/* d1 LIKE d2, d1 too large for d2 */
	return(1);

    for (i = 0; i < d1->cd_Bytes; ++i) {
	if (tolower(d1->cd_Data[i]) == tolower(d2->cd_Data[i]))
	    continue;
	return(2);			/* substring mismatch */
    }
    return(-1);				/* GOOD */
}

static int
OpLikeStrMatch(const ColData *d2, const ColData *d1)
{
    int i;

    if (d1->cd_Bytes > d2->cd_Bytes)	/* d1 LIKE d2, d1 too large for d2 */
	return(-1);

    for (i = 0; i < d1->cd_Bytes; ++i) {
	if (tolower(d1->cd_Data[i]) == tolower(d2->cd_Data[i]))
	    continue;
	return(-2);			/* substring mismatch */
    }
    return(1);				/* GOOD */
}

static int
OpSameStrMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	unsigned char c1 = tolower(d1->cd_Data[i]);
	unsigned char c2 = tolower(d2->cd_Data[i]);
	if (c1 == c2)
	    continue;
	if (c1 < c2)
	    return(-1);		/* FALSE region #1 (d1 smaller) */
	return(-2);		/* FALSE region #2 (d1 larger) */
    }
    if (s == d1->cd_Bytes) {
	if (s == d2->cd_Bytes)
	    return(1);		/* TRUE region #1 */
	return(-1);		/* FALSE region #1 (d1 smaller) */
    }
    return(-2);			/* FALSE region #2 (d1 larger) */
}

static int 
OpSameNoStrMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	unsigned char c1 = tolower(d1->cd_Data[i]);
	unsigned char c2 = tolower(d2->cd_Data[i]);
	if (c1 == c2)
	    continue;
	if (c1 < c2)
	    return(1);		/* TRUE region #1 (d1 smaller) */
	return(2);		/* TRUE region #2 (d1 larger) */
    }
    if (s == d1->cd_Bytes) {
	if (s == d2->cd_Bytes)
	    return(-1);		/* FALSE region #1 (exact match) */
	return(1);		/* TRUE region #1 (d1 smaller) */
    }
    return(2);			/* TRUE region #2 (d1 larger) */
}

