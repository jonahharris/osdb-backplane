/*
 * LIBCLIENT/ERROR.C
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libclient/error.c,v 1.5 2002/08/20 22:05:48 dillon Exp $
 */

#include "defs.h"

static const char UnknownError[] = "Unknown Error Code";
static const char *ErrorStrings[] = { ERROR_STRINGS };

Export const char *GetCLErrorDesc(int errorCode);

/* GetCLErrorDesc - Return description string for database error code
 *
 * Arguments:	errorCode	Database error code
 * Globals:	ErrorStrings	Array of strings that describe database errors
 *
 * Returns:	Database error description if error code is in range
 *		Invalid database error code message if not
 *
 * Description:	Given a valid database error code, GetCLErrorDesc returns
 *		a string that describes the error. The error code can be
 *		greater or less than 0; GetCLErrorDesc will return the same
 *		description for error codes 5 and -5. If the error code is
 *		out of range, GetCLErrorDesc returns message to this effect. 
 */ 
const char *
GetCLErrorDesc(int errorCode)
{
    static char *invalidCode = "Database error code is out of range";

    if (abs(errorCode) < arysize(ErrorStrings))
	return(ErrorStrings[abs(errorCode)]);
    else
	return(invalidCode);
}
