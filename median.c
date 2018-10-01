/*-------------------------------------------------------------------------
 *
 * median.c
 *    A median aggregate implementation using a sorted array.
 *
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include <fmgr.h>
#include <math.h>
#include "utils/float.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"



#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
typedef struct varlena VarString;
#define DatumGetVarStringPP(X)                ((VarString *) PG_DETOAST_DATUM_PACKED(X))


typedef struct
{
	Size		alloc_len;		/* allocated length */
	Size		next_alloc_len; /* next allocated length */
	Size		nelems;			/* number of valid entries */
	Datum	   *d;
}			State;

int			float8_cmp(const void *a, const void *b);
int			timestamp_cmp(const void *a, const void *b);
int			text_cmp(const void *a, const void *b);


PG_FUNCTION_INFO_V1(median_transfn);
/*
 * Median state transfer function.
 *
 * This function accumulates all results into an unsorted array of
 * Datums and expands * that array as needed. All results must be
 * loaded into memory (no spilling to disk).
 * */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	State	   *state = (State *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Datum		value;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	if (PG_ARGISNULL(1))
		PG_RETURN_BYTEA_P(state);

	value = PG_GETARG_DATUM(1);


	if (state == NULL)
	{
		state = MemoryContextAllocZero(agg_context, sizeof(State));
		state->d = MemoryContextAllocZero(agg_context, sizeof(Datum) * 1024);
		state->alloc_len = 1024;
		state->next_alloc_len = 2048;
		state->nelems = 0;
	}
	else if (state->nelems >= state->alloc_len)
	{
		MemoryContext old_context = MemoryContextSwitchTo(agg_context);
		int			newlen = state->next_alloc_len;

		state->next_alloc_len += state->alloc_len;
		state->alloc_len = newlen;
		state->d = repalloc(state->d, state->alloc_len * sizeof(Datum));
		MemoryContextSwitchTo(old_context);
	}

	state->d[state->nelems++] = value;

	PG_RETURN_BYTEA_P(state);
}

/* type-specific comparison functions */
int
float8_cmp(const void *a, const void *b)
{
	float8		af = DatumGetFloat8(*(Datum *) a);
	float8		bf = DatumGetFloat8(*(Datum *) b);

	return float8_cmp_internal(af, bf);
}

int
timestamp_cmp(const void *a, const void *b)
{
	Timestamp	at = DatumGetTimestamp(*(Datum *) a);
	Timestamp	bt = DatumGetTimestamp(*(Datum *) b);

	return timestamptz_cmp_internal(at, bt);
}

int
text_cmp(const void *a, const void *b)
{
	Datum		ad = *(Datum *) a;
	Datum		bd = *(Datum *) b;
	struct varlena *arg1 = DatumGetVarStringPP(ad);
	struct varlena *arg2 = DatumGetVarStringPP(bd);
	char	   *a1p,
			   *a2p;
	int			len1,
				len2,
				result;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	result = memcmp(a1p, a2p, Min(len1, len2));
	if ((result == 0) && (len1 != len2))
		result = (len1 < len2) ? -1 : 1;

	/* We can't afford to leak memory here. */
	if (PointerGetDatum(arg1) != ad)
		pfree(arg1);
	if (PointerGetDatum(arg2) != bd)
		pfree(arg2);

	return result;
}

PG_FUNCTION_INFO_V1(median_double_finalfn);

/*
 * Median final function for doubles.
 *
 * This sorts the accumulated array of datums and returns the median.
 */
Datum
median_double_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	State	   *state = (State *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Size		low_idx,
				hi_idx;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (state == NULL)
		PG_RETURN_NULL();

	qsort(state->d, state->nelems, sizeof(Datum), float8_cmp);

	low_idx = state->nelems / 2;
	hi_idx = state->nelems / 2 - 1;

	if (low_idx == hi_idx)
		PG_RETURN_FLOAT8(DatumGetFloat8(state->d[low_idx]));
	else
		PG_RETURN_FLOAT8(
						 (DatumGetFloat8(state->d[low_idx]) +
						  DatumGetFloat8(state->d[hi_idx])) / 2);
}

PG_FUNCTION_INFO_V1(median_timestamptz_finalfn);

/*
 * Median final function for timestamps.
 *
 * This sorts the accumulated array of datums and returns the median.
 */
Datum
median_timestamptz_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	State	   *state = (State *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (state == NULL)
		PG_RETURN_NULL();

	qsort(state->d, state->nelems, sizeof(Datum), timestamp_cmp);

	PG_RETURN_TIMESTAMPTZ(DatumGetTimestamp(state->d[state->nelems / 2]));
}

PG_FUNCTION_INFO_V1(median_text_finalfn);

/*
 * Median final function for text.
 *
 * This sorts the accumulated array of datums and returns the median.
 */
Datum
median_text_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	State	   *state = (State *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	if (state == NULL)
		PG_RETURN_NULL();

	qsort(state->d, state->nelems, sizeof(Datum), text_cmp);

	PG_RETURN_TEXT_P(DatumGetTextPP(state->d[state->nelems / 2]));
}
