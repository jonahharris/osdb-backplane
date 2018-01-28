/*
 * CACHE.H
 *
 * (c)Copyright 1999-2002 Backplane, Inc.  Please refer to the COPYRIGHT
 * file at the base of the distribution tree.
 *
 * Macros for implementing simple data caches.
 *
 * $Backplane: rdbms/libsupport/cache.h,v 1.2 2002/08/20 22:05:54 dillon Exp $
 */

#define CACHE_DECLARE(NAME)						\
    static SimpleHash cache_ ## NAME;					\
    static int cache_ ## NAME ## _init = 0				\


#define CACHE_LOOKUP_STRING(NAME, KEY, RESULT) do {			\
    if (!cache_ ## NAME ## _init) {					\
	simpleHashInit(&cache_ ## NAME);					\
	cache_ ## NAME ## _init = 1;					\
    }									\
    else {								\
	RESULT = simpleHashLookup(&cache_ ## NAME, KEY);		\
	if (RESULT != NULL)						\
	    RESULT = safe_strdup(RESULT);				\
    }									\
} while (0)


#define CACHE_ADD_STRING(NAME, KEY, VALUE) do {				\
    char *oldvalue;							\
    oldvalue = simpleHashEnter(&cache_ ## NAME, KEY,safe_strdup(VALUE));\
    safe_free(&oldvalue);						\
} while (0)


/*
 * Note that when caching integers, it is much harder to detect no match
 * and a match with a value of zero. If possible, refrain from storing a
 * value of zero in the cache to avoid this problem.
 */

#define CACHE_LOOKUP_INT(NAME, KEY, RESULT) do {			\
    if (!cache_ ## NAME ## _init) {					\
	simpleHashInit(&cache_ ## NAME);				\
	cache_ ## NAME ## _init = 1;					\
    }									\
    else {								\
	RESULT = (intptr_t)simpleHashLookup(&cache_ ## NAME, KEY);	\
    }									\
} while (0)


#define CACHE_ADD_INT(NAME, KEY, VALUE)					\
    simpleHashEnter(&cache_ ## NAME, KEY, (void *)(intptr_t)(VALUE))
