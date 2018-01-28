/*
 * PHP_BACKPLANE.H
 *
 * $Backplane: rdbms/php4/php_backplane.h,v 1.1 2003/04/26 18:02:42 dillon Exp $
 */

#include <php/main/php.h>		/* PHP includes	*/
#include <php/main/php_globals.h>
#include <php/ext/standard/info.h>
#include <php/ext/standard/php_string.h>

#include <sys/types.h>			/* System includes */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <signal.h>
#include <signal.h>
#include <netdb.h>
#include <netinet/in.h>

#include <php/main/php_ini.h>		/* PHP include	*/

#include <libdbcore/types.h>		/* Backplane includes */
#include <libsupport/export.h>
#include <rdbms/errors.h>
#include <libclient/pexport.h>

/*
 * A backplane connection as represented by php
 */

struct bkpl_conn;

typedef struct bkpl_trans {
    struct bkpl_trans	*parent;	/* nesting parent or NULL */
    struct bkpl_trans	*def_parent;	/* default stacking */
    struct bkpl_conn	*bkpl;
    res_t		res;		/* current result being processed */
    int			id;
    int			errnum;
    int			rowno;
    const char		*error;
    char		is_streaming;	/* streaming instance */
} bkpl_trans;

typedef struct bkpl_conn {
    database_t		dbc;		/* primary database connection */
    database_t		dbi;
    dbstamp_t		fts;
    struct bkpl_trans	*trans;
    int			id;		/* PHP resource id */
    int			htype;		/* PHP resource type (link or plink) */
    HashTable		*hash;
    char		*user;		/* database user */
    char		*dbname;	/* database name */
} bkpl_conn;

extern zend_module_entry bkpl_module_entry;

ZEND_BEGIN_MODULE_GLOBALS(bkpl)
    struct bkpl_trans *def_trans;
    long num_links,num_persistent;	/* XXX */
    long max_links,max_persistent;	/* XXX */
    long allow_persistent;		/* XXX */
    long default_port;			/* XXX */
    char *default_host;			/* XXX */
    char *default_user;			/* XXX */
    char *default_password;		/* XXX */
    char *default_socket;		/* XXX */
    char *connect_error;		/* XXX */
    long connect_errno;			/* XXX */
    long connect_timeout;		/* XXX */
    long result_allocated;		/* XXX */
    long trace_mode;			/* XXX */
ZEND_END_MODULE_GLOBALS(bkpl)

#ifdef ZTS
# define MySG(v) TSRMG(bkpl_globals_id, zend_bkpl_globals *, v)
#else
# define MySG(v) (bkpl_globals.v)
#endif

