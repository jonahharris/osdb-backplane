/*
 * DBDIR.C
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * $Backplane: rdbms/libsupport/dbdir.c,v 1.5 2002/08/20 22:05:54 dillon Exp $
 */

#include "defs.h"

Export const char *DefaultDBDir(void);
Export void SetDefaultDBDir(const char *dbdir);

static char *BaseDir;

/*
 * DefaultDBDir() - return database base directory
 *
 *	This extracts the database base directory from the environment and
 *	returns it.  If no database base directory exists in the environment,
 *	a global default is returned.
 *
 *	In anycase, SetDefaultDBDir() will override any global or 
 *	environmental default.
 */
const char *
DefaultDBDir(void)
{
    if (BaseDir == NULL) {
	const char *dbdir;

	if ((dbdir = getenv("RDBMS_DIR")) == NULL)
	    dbdir = DEFAULT_DBDIR;
	BaseDir = safe_strdup(dbdir);
    }
    return(BaseDir);
}

/*
 * SetDefaultDBDir() - set the database base directory
 *
 *	This function overrides the database base directory returned by
 *	DefaultDBDir() to the specified argument.  The passed string will
 *	be copied and can be thrown away by the caller on return.
 *
 *	The caller may pass NULL, causing DefaultDBDir() to revert to
 *	its default operation (i.e. revert to using the environment or
 *	global default).
 */
void
SetDefaultDBDir(const char *dbdir)
{
    safe_replace(&BaseDir, dbdir);
}

