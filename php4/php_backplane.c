/*
 * PHP_BACKPLANE.C
 *
 * This module implements a PHP4 interface for the Backplane database.  While
 * most traditional php database interfaces appears to focus on a kitchen-sink
 * approach that mimics the original msql API, the backplane interface focuses
 * on a more structured approach that takes full advantage of the backplane
 * database's nesting capabilities.  Unlike many of its counterparts, the
 * backplane database is fully capable of doing nested commits and rollbacks
 * which makes it utterly trivial to subroutinize database support functions
 * in PHP.
 *
 * $Backplane: rdbms/php4/php_backplane.c,v 1.5 2003/04/29 06:56:32 dillon Exp $
 */

#include "php_backplane.h"

PHP_MINIT_FUNCTION(bkpl);
PHP_RINIT_FUNCTION(bkpl);
PHP_MSHUTDOWN_FUNCTION(bkpl);
PHP_RSHUTDOWN_FUNCTION(bkpl);
PHP_MINFO_FUNCTION(bkpl);

PHP_FUNCTION(bkpl_connect);
PHP_FUNCTION(bkpl_pconnect);
PHP_FUNCTION(bkpl_close);
PHP_FUNCTION(bkpl_begin);
PHP_FUNCTION(bkpl_beginro);
PHP_FUNCTION(bkpl_rollback);
PHP_FUNCTION(bkpl_commit);
PHP_FUNCTION(bkpl_query);
PHP_FUNCTION(bkpl_rewind);
PHP_FUNCTION(bkpl_fetch_row);
PHP_FUNCTION(bkpl_error);
PHP_FUNCTION(bkpl_errno);
PHP_FUNCTION(bkpl_ping);

function_entry backplane_functions[] = {
    PHP_FE(bkpl_connect,		NULL)
    PHP_FE(bkpl_pconnect,		NULL)
    PHP_FE(bkpl_close,			NULL)
    PHP_FE(bkpl_begin,			NULL)
    PHP_FE(bkpl_beginro,		NULL)
    PHP_FE(bkpl_rollback,		NULL)
    PHP_FE(bkpl_commit,			NULL)
    PHP_FE(bkpl_query,			NULL)
    PHP_FE(bkpl_rewind,			NULL)
    PHP_FE(bkpl_fetch_row,		NULL)
    PHP_FE(bkpl_error,			NULL)
    PHP_FE(bkpl_errno,			NULL)
    PHP_FE(bkpl_ping,			NULL)
    {NULL, NULL, NULL}
};

zend_module_entry bkpl_module_entry = {
    STANDARD_MODULE_HEADER,
    "bkpl",
    backplane_functions,
    ZEND_MODULE_STARTUP_N(bkpl),
    PHP_MSHUTDOWN(bkpl),
    PHP_RINIT(bkpl),
    PHP_RSHUTDOWN(bkpl),
    PHP_MINFO(bkpl),
    NO_VERSION_YET,
    STANDARD_MODULE_PROPERTIES
};

ZEND_DECLARE_MODULE_GLOBALS(bkpl)

ZEND_GET_MODULE(bkpl)

/*
 * We register various resource types in the init and attach destructors to
 * them for cleanup purposes.
 */
static int le_result = -1;
static int le_link = -1;	/* database_t resource / current connection */
static int le_plink = -1;	/* database_t resource / persistent connect */
static int le_trans = -1;	/* current transactions and query */

static void freeBkplConn(bkpl_conn *bkpl);
static int beginBkplTrans(bkpl_conn *bkpl, int flags TSRMLS_DC);
static int endBkplTrans(bkpl_trans *trans, int commit TSRMLS_DC);

static
void
_close_bkpl_link(zend_rsrc_list_entry *info TSRMLS_DC)
{
    freeBkplConn((bkpl_conn *)info->ptr TSRMLS_CC);
}

static
void
_close_bkpl_plink(zend_rsrc_list_entry *info TSRMLS_DC)
{
    _close_bkpl_link(info TSRMLS_CC);
    --MySG(num_persistent);
}

/*
 * Initialization management array
 */
PHP_INI_BEGIN()
	STD_PHP_INI_BOOLEAN(
	    "bkpl.allow_persistent",	"1",	PHP_INI_SYSTEM,
	    OnUpdateInt,	allow_persistent,	zend_bkpl_globals,
	    bkpl_globals
	)
	STD_PHP_INI_ENTRY_EX(
	    "bkpl.max_persistent",	"-1",	PHP_INI_SYSTEM,
	    OnUpdateInt,	max_persistent,		zend_bkpl_globals,
	    bkpl_globals,	display_link_numbers
	)
	STD_PHP_INI_ENTRY_EX(
	    "bkpl.max_links",		"-1",	PHP_INI_SYSTEM,
	    OnUpdateInt,	max_links,		zend_bkpl_globals,
	    bkpl_globals,	display_link_numbers
	)
	STD_PHP_INI_ENTRY(
	    "bkpl.default_host",	NULL,	PHP_INI_ALL,
	    OnUpdateString,	default_host,		zend_bkpl_globals,
	    bkpl_globals
	)
	STD_PHP_INI_ENTRY(
	    "bkpl.default_user",	NULL,	PHP_INI_ALL,
	    OnUpdateString,	default_user,		zend_bkpl_globals,
	    bkpl_globals
	)
	STD_PHP_INI_ENTRY(
	    "bkpl.default_password",	NULL,	PHP_INI_ALL,
	    OnUpdateString,	default_password,	zend_bkpl_globals,
	    bkpl_globals
	)
	STD_PHP_INI_ENTRY(
	    "bkpl.default_socket",	NULL,	PHP_INI_ALL,
	    OnUpdateStringUnempty,	default_socket,	zend_bkpl_globals,
	    bkpl_globals
	)
	STD_PHP_INI_ENTRY(
	    "bkpl.connect_timeout",	"-1",	PHP_INI_SYSTEM,
	    OnUpdateInt,		connect_timeout, zend_bkpl_globals,
	    bkpl_globals
	)
	STD_PHP_INI_BOOLEAN(
	    "bkpl.trace_mode",		"0",	PHP_INI_ALL,
	    OnUpdateInt,		trace_mode, 	zend_bkpl_globals,
	    bkpl_globals
	)
PHP_INI_END()

/*
 * php_bkpl_init_globals()
 *
 *	Initialize
 */
static
void
php_bkpl_init_globals(zend_bkpl_globals *gs)
{
    gs->num_persistent = 0;
    gs->default_socket = NULL;
    gs->default_host = NULL;
    gs->default_user = NULL;
    gs->default_password = NULL;
    gs->connect_errno = 0;
    gs->connect_error = NULL;
    gs->connect_timeout = 0;
    gs->trace_mode = 0;
    gs->result_allocated = 0;
}

/*
 * ZEND_MODULE_STARTUP_D
 *
 *	Startup procedure.  Note passed arguments: type, module_number, and
 *	TSRMLS_D.
 *
 *	We must primarily register the resource types we will be using
 *	to hold persistent and semi-persistent information.
 */

ZEND_MODULE_STARTUP_D(bkpl)
{
    ZEND_INIT_MODULE_GLOBALS(bkpl, php_bkpl_init_globals, NULL);
    REGISTER_INI_ENTRIES();

    /*
     * Register datatypes for destructor cleanup
     */
    le_result = zend_register_list_destructors_ex(
		    NULL, NULL, "bkpl result", module_number);
    le_trans = zend_register_list_destructors_ex(
		    NULL, NULL, "bkpl transaction or subtransaction",
		    module_number);
    le_link = zend_register_list_destructors_ex(
		    _close_bkpl_link, NULL, "bkpl link", module_number);
    le_plink = zend_register_list_destructors_ex(
		    NULL, _close_bkpl_plink, "bkpl link persistent",
		    module_number);
    Z_TYPE(bkpl_module_entry) = type;	/* type passed as argument */

    return SUCCESS;
}

/*
 * PHP_MSHUTDOWN_FUNCTION
 *
 *	Shutdown procedure
 */

PHP_MSHUTDOWN_FUNCTION(bkpl)
{
    UNREGISTER_INI_ENTRIES();
    return SUCCESS;
}

/*
 * PHP_RINIT_FUNCTION
 *
 *	Reinit function?.  Note that the connection error and error
 *	number is reset on every request.
 */
PHP_RINIT_FUNCTION(bkpl)
{
    MySG(def_trans)= NULL;
    MySG(num_links) = MySG(num_persistent);
    MySG(connect_error) = NULL;
    MySG(connect_errno) = 0;
    MySG(trace_mode) = 0;
    MySG(result_allocated) = 0;

    return SUCCESS;
}

/*
 * PHP_RSHUTDOWN_FUNCTION()
 *
 *	Transitory cleanup function?
 */
PHP_RSHUTDOWN_FUNCTION(bkpl)
{
    if (MySG(trace_mode)) {
	if (MySG(result_allocated)) {
	    char tmp[128];

	    snprintf(tmp, sizeof(tmp),
		"%lu result set(s) not freed.  Use bkpl_free_result to "
		"free result sets which were requested using bkpl_query()",
		MySG(result_allocated)
	    );
	    php_error_docref("function.bkpl-free-result" TSRMLS_CC,
		E_WARNING, tmp);
	}
    }

    if (MySG(connect_error) != NULL) {
	free(MySG(connect_error));
	MySG(connect_error) = NULL;
    }
    return SUCCESS;
}

/*
 * PHP_MINFO_FUNCTION()
 */
PHP_MINFO_FUNCTION(bkpl)
{
    char buf[32];

    php_info_print_table_start();
    php_info_print_table_header(2, "Backplane Support", "enabled");
    sprintf(buf, "%ld", MySG(num_persistent));
    php_info_print_table_row(2, "Active Persistent Links", buf);
    sprintf(buf, "%ld", MySG(num_links));
    php_info_print_table_row(2, "Active Links", buf);
    php_info_print_table_end();

    DISPLAY_INI_ENTRIES();
}

/************************************************************************
 *			STATIC SUPPORT FUNCTIONS			*
 ************************************************************************/

/*
 * getBkplConn() -	retrieve the backplane connection represented
 *			by the identifier.
 */
bkpl_conn *
getBkplConn(int id TSRMLS_DC)
{
    int htype;
    bkpl_conn *bkpl;

    if (id == -1)
	return(NULL);
    if ((bkpl = zend_list_find(id, &htype)) == NULL)
	return(NULL);
    if (htype == le_link || htype == le_plink)
	return(bkpl);
    return(NULL);
}

bkpl_conn *
getBkplConnZ(zval **z_id TSRMLS_DC)
{
    if (z_id == NULL)
	return(NULL);
    if (Z_TYPE_PP(z_id) != IS_RESOURCE)
	return(NULL);
    return(getBkplConn(Z_RESVAL_PP(z_id) TSRMLS_CC));
}

/*
 * getBkplTrans() -	retrieve the backplane transaction represented
 *			by the identifier.
 */
bkpl_trans *
getBkplTrans(int id TSRMLS_DC)
{
    int htype;
    bkpl_trans *trans;

    if (id == -1)
	return(NULL);
    if ((trans = zend_list_find(id, &htype)) == NULL)
	return(NULL);
    if (htype == le_trans)
	return(trans);
    return(NULL);
}

bkpl_trans *
getBkplTransZ(zval **z_id TSRMLS_DC)
{
    if (z_id == NULL)
	return(NULL);
    if (Z_TYPE_PP(z_id) != IS_RESOURCE)
	return(NULL);
    return(getBkplTrans(Z_RESVAL_PP(z_id) TSRMLS_CC));
}

/*
 * createBkplConn()
 *
 *	Create a connection to a particular backplane database.
 */
static
int
createBkplConn(const char *dbname, const char *rdbmsdir, int persistent TSRMLS_DC)
{
    int r;
    bkpl_conn *bkpl;
    HashTable *hash;
    int htype;

    SetDefaultDBDir(rdbmsdir);	/* Note: may be NULL */

#if 0
    /*
     * Handle safe mode
     */
    if (PG(sql_safe_mode)) {
	...
    }
#endif

    /*
     * Deal with persitence.  Persistence determines which resource list
     * we use for the link.  We may still cache the link within the script.
     */
    if (!MySG(allow_persistent))
	persistent = 0;
    if (persistent) {
	hash = &EG(persistent_list);
	htype = le_plink;
    } else {
	hash = &EG(regular_list);
	htype = le_link;
    }

#if 0
    /*
     * Generate a hash key and see if we already have a cached link.  If we
     * cannot find a cached link or a new link is being requested explicitly,
     * then check our link count limits and create a new link.
     *
     * We cannot actually open the database until we have a database name.
     */
    snprintf(hstr, sizeof(hstr), "backplane_%s", user);

    if (nlink == 0 && 
	zend_hash_find(hash, hstr, strlen(hstr) + 1, (void **)&le) == SUCCESS
    ) {
	assert(Z_TYPE_P(le) == htype);
	bkpl = le->ptr;
	return(bkpl->id);
    }
#endif

    if (MySG(max_links) != -1 && MySG(num_links) >= MySG(max_links)) {
	php_error_docref(NULL TSRMLS_CC, E_WARNING, 
	    "Too many open links (%d)", MySG(num_links));
	return(-1);
    }
    if (persistent &&
	MySG(max_persistent) != -1 &&
	MySG(num_persistent) >= MySG(max_persistent)
    ) {
	php_error_docref(NULL TSRMLS_CC, E_WARNING, 
	    "Too many open persistent links (%d)", MySG(num_persistent));
	return(-1);
    }
    bkpl = malloc(sizeof(bkpl_conn));
    bzero(bkpl, sizeof(bkpl_conn));

#if 0
    Z_TYPE(le) = htype;
    le.ptr = bkpl;
    r = zend_hash_update(hash, hstr, strlen(hstr) + 1, 
	(void *)&le, sizeof(list_entry), NULL);
    if (r == FAILURE) {
	free(bkpl);
	return(-1);
    }
#endif
    bkpl->dbname = strdup(dbname);
    bkpl->htype = htype;
    bkpl->hash = hash;
    bkpl->dbc = OpenCLDataBase(dbname, &r);
    bkpl->id = -1;
    if (bkpl->dbc == NULL) {
	freeBkplConn(bkpl);
	php_error_docref(NULL TSRMLS_CC, E_WARNING, 
	    "Unable to open backplane database(%s)", dbname);
	return(-1);
    }
    if (persistent) {
	MySG(num_persistent)++;
    }
    MySG(num_links)++;
    bkpl->id = ZEND_REGISTER_RESOURCE(NULL, bkpl, htype);
    return(bkpl->id);
}

/*
 * freeBkplConn() - free the specified backplane link
 *
 *	Any transactions related to the link will be rolled-back prior to
 *	the link being freed.
 */
static
void
freeBkplConn(bkpl_conn *bkpl)
{
    /*
     * We have to pop our transaction stack until the requested backplane
     * connection has no more active transactions.
     */
    while (bkpl->trans != NULL) {
	endBkplTrans(MySG(def_trans), 0 TSRMLS_CC);
    }
    if (bkpl->dbi) {
	CloseCLInstance(bkpl->dbi);
	bkpl->dbi = NULL;
    }
    if (bkpl->dbc) {
	CloseCLDataBase(bkpl->dbc);
	bkpl->dbc = NULL;
    }
    if (bkpl->user) {
	free(bkpl->user);
	bkpl->user = NULL;
    }
    if (bkpl->dbname) {
	free(bkpl->dbname);
	bkpl->dbname = NULL;
    }
    free(bkpl);
    --MySG(num_links);
}

/*
 * beginBkplTrans() - push a new transaction with appropriate CPF_ flags.
 */
static
int
beginBkplTrans(bkpl_conn *bkpl, int flags TSRMLS_DC)
{
    bkpl_trans *trans;

    if (bkpl == NULL)
	return(-1);

    if (bkpl->dbi == NULL) {
	bkpl->dbi = OpenCLInstance(bkpl->dbc, &bkpl->fts, CLTYPE_RW);
	if (bkpl->dbi == NULL)
	    return(-1);
    }
    PushCLTrans(bkpl->dbi, bkpl->fts, flags);

    trans = malloc(sizeof(bkpl_trans));
    bzero(trans, sizeof(bkpl_trans));
    trans->parent = bkpl->trans;
    trans->bkpl = bkpl;
    bkpl->trans = trans;

    trans->def_parent = MySG(def_trans);
    MySG(def_trans) = trans;

    trans->id = ZEND_REGISTER_RESOURCE(NULL, trans, le_trans);
    return(trans->id);
}

static
int
endBkplTrans(bkpl_trans *trans, int commit TSRMLS_DC)
{
    int r;

    assert(trans->bkpl->trans == trans);
    assert(MySG(def_trans) == trans);
    if (trans->res) {
	FreeCLRes(trans->res);
	trans->res = NULL;
    }
    if (commit) {
	r = Commit1CLTrans(trans->bkpl->dbi, &trans->bkpl->fts);
	if (r == 0)
	    r = Commit2CLTrans(trans->bkpl->dbi, trans->bkpl->fts);
    } else {
	AbortCLTrans(trans->bkpl->dbi);
	r = 0;
    }
    MySG(def_trans) = trans->def_parent;
    trans->def_parent = NULL;
    trans->bkpl->trans = trans->parent;
    trans->parent = NULL;
    zend_list_delete(trans->id);
    free(trans);
    return(r);
}

/************************************************************************
 *				API FUNCTIONS				*
 ************************************************************************/

/*
 * bkpl_connect(database)
 */
PHP_FUNCTION(bkpl_connect)
{
    zval **z_dbname;
    zval **z_rdbmsdir = NULL;
    char *dbname;
    char *rdbmsdir;
    int id;
    int r;

    switch(ZEND_NUM_ARGS()) {
    case 2:
	r = zend_get_parameters_ex(2, &z_dbname, &z_rdbmsdir);
	break;
    case 1:
	r = zend_get_parameters_ex(1, &z_dbname);
	break;
    default:
	r = FAILURE;
	break;	/* NOT REACHED */
    }
    if (r == FAILURE) {
	zend_wrong_param_count(TSRMLS_C);
	RETURN_FALSE;
    }
    convert_to_string_ex(z_dbname);
    dbname = Z_STRVAL_PP(z_dbname);
    if (z_rdbmsdir) {
	convert_to_string_ex(z_rdbmsdir);
	rdbmsdir = Z_STRVAL_PP(z_rdbmsdir);
    } else {
	rdbmsdir = NULL;
    }

    id = createBkplConn(dbname, rdbmsdir, 0 TSRMLS_CC);
    if (id == -1) {
	RETVAL_FALSE;
    } else {
	RETVAL_RESOURCE(id);
    }
}

/*
 * bkpl_pconnect(database)
 */
PHP_FUNCTION(bkpl_pconnect)
{
    zval **z_dbname;
    zval **z_rdbmsdir;
    char *dbname;
    char *rdbmsdir;
    int id;
    int r;

    switch(ZEND_NUM_ARGS()) {
    case 2:
	r = zend_get_parameters_ex(2, &z_dbname, &z_rdbmsdir);
	break;
    case 1:
	r = zend_get_parameters_ex(1, &z_dbname);
	break;
    default:
	r = FAILURE;
	break;	/* NOT REACHED */
    }
    if (r == FAILURE) {
	zend_wrong_param_count(TSRMLS_C);
	RETURN_FALSE;
    }
    convert_to_string_ex(z_dbname);
    dbname = Z_STRVAL_PP(z_dbname);
    convert_to_string_ex(z_rdbmsdir);
    rdbmsdir = Z_STRVAL_PP(z_rdbmsdir);

    id = createBkplConn(dbname, rdbmsdir, 1 TSRMLS_CC);
    if (id == -1) {
	RETVAL_FALSE;
    } else {
	RETVAL_RESOURCE(id);
    }
}

/*
 * bkpl_close(linkid)
 *
 *	Note that bkpl_close() automatically pops as many transactions as 
 *	are necessary to clear the transaction stack associated with the
 *	linkid.
 */
PHP_FUNCTION(bkpl_close)
{
    zval **z_id = NULL;
    bkpl_conn *bkpl = NULL;

    switch (ZEND_NUM_ARGS()) {
    case 1:
	zend_get_parameters_ex(1, &z_id);
	bkpl = getBkplConnZ(z_id TSRMLS_CC);
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }

    if (bkpl == NULL)
	RETURN_FALSE;
#if 0
    snprintf(hstr, sizeof(hstr), "backplane_%s", bkpl->user);
    zend_hash_del(hash, hstr, strlen(hstr) + 1);
#endif
    zend_list_delete(bkpl->id);	/* this will call the destructor */
    RETVAL_TRUE;
}

/*
 * transid = bkpl_begin(linkid [, streaming])
 * transid = bkpl_beginro(linkid [, streaming])
 *
 *	Start a transaction or sub-transaction and return a transaction id.
 *	The transaction id becomes the current transaction for queries and
 *	must be supplied when you commit or roll-back a transaction as a
 *	sanity check against bugs in sub-transactions (you don't want to call
 *	a subroutine and return to a different current transaction then you
 *	were in prior to the subroutine call!).
 */
PHP_FUNCTION(bkpl_begin)
{
    zval **z_id = NULL;
    zval **z_streaming = NULL;
    bkpl_conn *bkpl = NULL;
    int id;
    int flags;

    switch (ZEND_NUM_ARGS()) {
    case 2:
	zend_get_parameters_ex(2, &z_id, &z_streaming);
	bkpl = getBkplConnZ(z_id TSRMLS_CC); 
	break;
    case 1:
	zend_get_parameters_ex(1, &z_id);
	bkpl = getBkplConnZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }

    flags = CPF_RWSYNC;
    if (z_streaming) {
	convert_to_long_ex(z_streaming);
	if (Z_LVAL_PP(z_streaming) & 1)
	    flags |= CPF_STREAM;
    }
    if ((id = beginBkplTrans(bkpl, flags TSRMLS_CC)) == -1) {
	RETVAL_FALSE;
    } else {
	RETVAL_RESOURCE(id);
    }
}

PHP_FUNCTION(bkpl_beginro)
{
    zval **z_id = NULL;
    zval **z_streaming = NULL;
    bkpl_conn *bkpl = NULL;
    int id;
    int flags;

    switch (ZEND_NUM_ARGS()) {
    case 2:
	zend_get_parameters_ex(2, &z_id, &z_streaming);
	bkpl = getBkplConnZ(z_id TSRMLS_CC); 
	break;
    case 1:
	zend_get_parameters_ex(1, &z_id);
	bkpl = getBkplConnZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    flags = CPF_READONLY | CPF_RWSYNC;
    if (z_streaming) {
	convert_to_long_ex(z_streaming);
	if (Z_LVAL_PP(z_streaming) & 1)
	    flags |= CPF_STREAM;
    }
    if ((id = beginBkplTrans(bkpl, flags TSRMLS_CC)) == -1) {
	RETVAL_FALSE;
    } else {
	RETVAL_RESOURCE(id);
    }
}

/*
 * bkpl_rollback([transid])
 *
 *	Pop the default stack through the current transaction related to the
 *	linkid, cleaning up any active result sets, and then terminate the
 *	current transaction with a rollback.
 */
PHP_FUNCTION(bkpl_rollback)
{
    zval **z_id = NULL;
    bkpl_trans *trans;

    switch (ZEND_NUM_ARGS()) {
    case 0:
	trans = MySG(def_trans);
	break;
    case 1:
	zend_get_parameters_ex(1, &z_id);
	trans = getBkplTransZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    if (trans == NULL)
	RETURN_FALSE;
    while (MySG(def_trans) != trans)
	endBkplTrans(MySG(def_trans), 0 TSRMLS_CC);
    if (endBkplTrans(trans, 0 TSRMLS_CC) == 0) {
	RETVAL_TRUE;
    } else {
	RETVAL_FALSE;
    }
}

/*
 * bkpl_commit([transid])
 *
 *	Commit and terminate the current transaction.  The default transaction
 *	becomes the parent transaction (or none).
 */
PHP_FUNCTION(bkpl_commit)
{
    zval **z_id = NULL;
    bkpl_trans *trans;

    switch (ZEND_NUM_ARGS()) {
    case 0:
	trans = MySG(def_trans);
	break;
    case 1:
	zend_get_parameters_ex(1, &z_id);
	trans = getBkplTransZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    if (trans == NULL)
	RETURN_FALSE;
    while (MySG(def_trans) != trans)
	endBkplTrans(MySG(def_trans), 0 TSRMLS_CC);
    if (endBkplTrans(trans, 1 TSRMLS_CC) == 0) {
	RETVAL_TRUE;
    } else {
	RETVAL_FALSE;
    }
}

/*
 * bkpl_query(query[, transid])
 *
 *	Execute a backplane query, replacing the current result (if any) with
 *	a new result.
 */
PHP_FUNCTION(bkpl_query)
{
    zval **z_id = NULL;
    zval **z_query = NULL;
    bkpl_trans *trans = NULL;
    char *query;

    switch (ZEND_NUM_ARGS()) {
    case 1:
	zend_get_parameters_ex(1, &z_query);
	trans = MySG(def_trans);
	break;
    case 2:
	zend_get_parameters_ex(2, &z_query, &z_id);
	trans = getBkplTransZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    if (trans == NULL)
	RETURN_FALSE;
    if (trans->res) {
	FreeCLRes(trans->res);
	trans->res = NULL;
    }
    trans->errnum = 0;
    trans->error = "<no_error>";
    trans->rowno = -1;

    convert_to_string_ex(z_query);
    query = Z_STRVAL_PP(z_query);
    trans->res = QueryCLTrans(trans->bkpl->dbi, query, &trans->errnum);
    if (trans->errnum < 0)
	trans->error = GetCLErrorDesc(trans->errnum);
    RETVAL_LONG(trans->errnum);
}

/*
 * bkpl_rewind()
 *
 *	Rewind our results to the beginning.
 */
PHP_FUNCTION(bkpl_rewind)
{
    bkpl_trans *trans = MySG(def_trans);

    if (trans == NULL)
	RETURN_FALSE;
    if (trans->res == NULL)
	RETURN_FALSE;
    trans->rowno = -1;
    RETURN_TRUE;
}

/*
 * bkpl_fetch_row()
 *
 *	Fetch the next row result from a query.  Return an array representing
 *	the row or a boolean FALSE if no rows remain.
 */
PHP_FUNCTION(bkpl_fetch_row)
{
    bkpl_trans *trans = MySG(def_trans);
    const char **row;
    int *lens;
    int i;
    int ncol;

    if (trans == NULL)
	RETURN_FALSE;
    if (trans->res == NULL)
	RETURN_FALSE;
    if (trans->rowno == -1)
	row = ResFirstRowL(trans->res, &lens);
    else
	row = ResNextRowL(trans->res, &lens);
    if (row == NULL)
	RETURN_FALSE;

    /*
     * Bump the current row number and extract the row into an array
     */
    ++trans->rowno;
    if (array_init(return_value) == FAILURE)
	RETURN_FALSE;
    ncol = ResColumns(trans->res);

    for (i = 0; i < ncol; ++i) {
	if (row[i]) {
	    add_index_stringl(return_value, i, (char *)row[i], lens[i], 1);
	} else {
	    add_index_null(return_value, i);
	}
    }

    /*
     *  note: associative:
     *
     *	add_assoc_stringl(retval, fieldname, row, rowlen, copy);
     *	add_assoc_null(return_value, fieldname);
     */
}

/*
 * bkpl_error([transid])
 */
PHP_FUNCTION(bkpl_error)
{
    zval **z_id = NULL;
    bkpl_trans *trans = NULL;

    switch (ZEND_NUM_ARGS()) {
    case 0:
	trans = MySG(def_trans);
	break;
    case 1:
	zend_get_parameters_ex(1, &z_id);
	trans = getBkplTransZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    if (trans == NULL)
	RETURN_FALSE;
}

/*
 * bkpl_errno([linkid])
 */
PHP_FUNCTION(bkpl_errno)
{
    zval **z_id = NULL;
    bkpl_trans *trans = NULL;

    switch (ZEND_NUM_ARGS()) {
    case 0:
	trans = MySG(def_trans);
	break;
    case 1:
	zend_get_parameters_ex(1, &z_id);
	trans = getBkplTransZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    if (trans == NULL)
	RETURN_FALSE;
}

/*
 * bkpl_ping(linkid)
 */
PHP_FUNCTION(bkpl_ping)
{
    zval **z_id = NULL;
    bkpl_conn *bkpl = NULL;

    switch (ZEND_NUM_ARGS()) {
    case 1:
	zend_get_parameters_ex(1, &z_id);
	bkpl = getBkplConnZ(z_id TSRMLS_CC); 
	break;
    default:
	WRONG_PARAM_COUNT;
	break;	/* NOT REACHED */
    }
    if (bkpl == NULL)
	RETURN_FALSE;
}

