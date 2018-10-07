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
#include <utils/float.h>
#include <utils/timestamp.h>
#include <utils/varlena.h>
#include <utils/lsyscache.h>
#include <utils/datum.h>



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
	bool		typ_by_val;		/* compare datum type by value */
	int16		typ_len;		/* datum type length */
	Oid			typ_id;			/* datum type oid */
	Datum	   *d;
}			State;

static inline void swap(Datum *a, Datum *b);
static int	float8_cmp(const void *a, const void *b);
static int	timestamp_cmp(const void *a, const void *b);
static int	text_cmp(const void *a, const void *b);


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
	MemoryContext agg_context,
				old_context;
	State	   *state = (State *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Datum		value;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	if (PG_ARGISNULL(1))
		PG_RETURN_BYTEA_P(state);

	value = PG_GETARG_DATUM(1);
	old_context = MemoryContextSwitchTo(agg_context);

	if (state == NULL)
	{
		Oid			datum_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

		if (!OidIsValid(datum_type))
			elog(ERROR, "could not determine data type of input");


		state = palloc(sizeof(State));
		state->d = palloc(sizeof(Datum) * 1024);
		state->alloc_len = 1024;
		state->next_alloc_len = 2048;
		state->nelems = 0;
		state->typ_id = datum_type;

		get_typlenbyval(datum_type, &state->typ_len, &state->typ_by_val);
	}
	else if (state->nelems >= state->alloc_len)
	{
		Size		newlen = state->next_alloc_len;

		state->next_alloc_len += state->alloc_len;
		state->alloc_len = newlen;
		state->d = repalloc(state->d, state->alloc_len * sizeof(Datum));
	}

	state->d[state->nelems++] = datumTransfer(value, state->typ_by_val, state->typ_len);
	MemoryContextSwitchTo(old_context);

	PG_RETURN_BYTEA_P(state);
}

/* type-specific comparison functions */
static int
float8_cmp(const void *a, const void *b)
{
	float8		af = DatumGetFloat8(*(Datum *) a);
	float8		bf = DatumGetFloat8(*(Datum *) b);

	return float8_cmp_internal(af, bf);
}

static int
timestamp_cmp(const void *a, const void *b)
{
	Timestamp	at = DatumGetTimestamp(*(Datum *) a);
	Timestamp	bt = DatumGetTimestamp(*(Datum *) b);

	return timestamptz_cmp_internal(at, bt);
}

static int
text_cmp(const void *a, const void *b)
{
	Datum		ad = *(Datum *) a;
	Datum		bd = *(Datum *) b;
	VarString  *arg1 = DatumGetVarStringPP(ad);
	VarString  *arg2 = DatumGetVarStringPP(bd);
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
	hi_idx = (state->nelems + 1) / 2 - 1;

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

PG_FUNCTION_INFO_V1(median_invfn);

/*
 * Median inverse function.
 *
 * This removes a datum from the array of accumulated values.
 */
Datum
median_invfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
	State	   *state = (State *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Datum		value;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_invfn called in non-aggregate context");

	if (PG_ARGISNULL(1))
		PG_RETURN_BYTEA_P(state);

	value = PG_GETARG_DATUM(1);

	/* TODO: use bsearch(3) here */
	for (Size i = 0; i < state->nelems; i++)
	{
		Datum		a = state->d[i],
					b = value;

		if (TypeIsToastable(state->typ_id))
		{
			a = (Datum) PG_DETOAST_DATUM(a);
			b = (Datum) PG_DETOAST_DATUM(b);
		}

		if (datumIsEqual(a, b, state->typ_by_val, state->typ_len))
		{
			swap(&state->d[i], &state->d[state->nelems - 1]);
			state->nelems--;
			PG_RETURN_BYTEA_P(state);
		}
	}
	elog(ERROR, "Value not found in median_invfn, must be a bug.");
}

static inline void
swap(Datum *a, Datum *b)
{
	Datum	   *t = a;

	*a = *b;
	*b = *t;
}
