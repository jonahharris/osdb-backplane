/*
 * TEST.E (embedded sql)
 */

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <libdbcore/types.h>
#include <libsupport/export.h>
/*#include <libthreads/export.h> (include if using backplane threads)*/
#include <rdbms/errors.h>
#include <libpclient/pexport.h> /* use libclient/export.h w/ Backplane thrds */

database_t Db;	/* root database handle */
database_t DbI;	/* database instance handle */
dbstamp_t  FTs;	/* freeze timestamp */

int
main(int ac, char **av)
{
    int eno;
    int status;

    Db = OpenCLDataBase("test", &eno);
    if (Db == NULL)
	fatal("OpenCLDataBase error %d (%s)\n", eno, GetCLErrorDesc(eno));
    DbI = OpenCLInstance(Db, &FTs, CLTYPE_RW);	/* or CLTYPE_RO */
    if (DbI == NULL)
	fatal("OpenCLInstance error\n");

    /*
     * Now some embedded cursors code
     *
     * CREATE SCHEMA test;
     * CREATE TABLE test.test ( data varchar, key varchar primary key );
     * LOAD INSERT INTO test.test ( key, data ) {
     * 	   VALUES ('key1', 'data1' );
     * 	   VALUES ('key2', 'data2' );
     * }
     */
    BEGIN(DbI, FTs, status) {
	SELECT t.key, t.data FROM test.test=t WHERE t.key > '0';
	{
	    printf("KEY %s DATA %s\n", t.key, t.data);
	    /*
	     * Transactions can be arbitrarily nested.  Any modifications you
	     * make in a nested transactions can be rolled-back by breaking
	     * out of it with ROLLBACK, or committed.  Committing a
	     * sub-transactions simply integrates the changes into the parent
	     * transaction, only a commit at the top level actually commits
	     * to the physical database.
	     */
	    /* BEGIN { ... } */

	    /*
	     * You can arbitrarily nest other queries, including other
	     * SELECTs.  Exception: You cannot nest queries if a streaming
	     * select is used.
	     */
	    UPDATE test.test SET t.data = 'x' WHERE t.data = 'y';
	}
	if (RESULT < 0)
	    printf("SELECT on test.test key and data failed\n");

	/*
	 * In this example I don't commit any of the updates I did.
	 */
	ROLLBACK;
    }
    /*
     * At this point status contains the return status.  0 indicates
     * a successful commit.
     *
     * The cursors implementation adjusts FTs to the commit time stamp + 1,
     * so any following queries will see changes made by prior queries.
     */
    return(0);
}


