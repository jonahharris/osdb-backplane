/*
 * UTILS/TEST.E
 *
 * (c)Copyright 2000-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 */

#include "defs.h"

void
task_main(int ac, char **av)
{
    database_t	db;
    database_t	dbin;
    int error;
    int status;
    dbstamp_t fts;

    if ((db = OpenCLDataBase("test", &error)) == NULL)
	fatal("Couldn't open database 'test' error %d", error);
    if ((dbin = OpenCLInstance(db, &fts, CLTYPE_RW)) == NULL)
	fatal("Couldn't open database instance");

    BEGIN(dbin, fts, status) READONLY {
	char *s = "sys";

	SELECT t.SchemaName, t.TableName, t.TableVId, t.TableFile
	FROM sys.tables=t WHERE t.SchemaName = s; 
	{
	    printf("Table %s.%s\tVId=%s\n",
		t.SchemaName,
		t.TableName,
		t.TableVId
	    );
	}
	printf("RESULT FROM SELECT IS %d\n", RESULT);
    }
    printf("RESULT FROM COMMIT IS %d (should be < 0 if readonly)\n", status);
}

