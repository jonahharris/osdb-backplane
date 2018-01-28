/*
 * LIBDBTYPES/DEFAULT.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 *	Implements string operators, validation, encoding, and decoding
 *	functions.
 *
 * $Backplane: rdbms/libdbtypes/default.c,v 1.3 2002/08/20 22:05:53 dillon Exp $
 */

#include "defs.h"

Prototype int OpUnknown(const ColData *d1, const ColData *d2);
Prototype int OpExactMatch(const ColData *d1, const ColData *d2);
Prototype int OpExactNoMatch(const ColData *d1, const ColData *d2);

Prototype dataop_func_t DataTypeUnknownFuncAry[];

dataop_func_t DataTypeUnknownFuncAry[] = {
	OpUnknown,		/* DATATYPE_UNKNOWN	*/
	OpUnknown,		/* DATATYPE_LIKE	*/
	OpUnknown,		/* DATATYPE_RLIKE	*/
	OpUnknown,		/* DATATYPE_SAME	*/
	OpUnknown,		/* DATATYPE_RSAME	*/
	OpUnknown,		/* DATATYPE_EQEQ	*/
	OpUnknown,		/* DATATYPE_NOTEQ	*/
	OpUnknown,		/* DATATYPE_LT		*/
	OpUnknown,		/* DATATYPE_LTEQ	*/
	OpUnknown,		/* DATATYPE_GT 		*/
	OpUnknown		/* DATATYPE_GTEQ	*/
};

int
OpUnknown(const ColData *d1, const ColData *d2)
{
    return(0);
}

int
OpExactMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	if (d1->cd_Data[i] == d2->cd_Data[i])
	    continue;
	if (d1->cd_Data[i] < d2->cd_Data[i])
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

int 
OpExactNoMatch(const ColData *d1, const ColData *d2)
{
    int s = (d1->cd_Bytes < d2->cd_Bytes) ? d1->cd_Bytes : d2->cd_Bytes;
    int i;

    for (i = 0; i < s; ++i) {
	if (d1->cd_Data[i] == d2->cd_Data[i])
	    continue;
	if (d1->cd_Data[i] < d2->cd_Data[i])
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

