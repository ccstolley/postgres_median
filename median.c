#include <postgres.h>
#include <fmgr.h>
#include <math.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


typedef struct
{
    int    alloc_len;      /* allocated length */
    int    next_alloc_len; /* next allocated length */
    int    nelems;            /* number of valid entries */
    union
    {
        float4    *float4_values;
        float8  *float8_values;
    } d;
} State;

int float8_cmp(const void *_a, const void *_b);


PG_FUNCTION_INFO_V1(median_transfn);



/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
    MemoryContext   agg_context;
    State          *state = (State*)(PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
    double          value;
    
    

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_transfn called in non-aggregate context");

    if (PG_ARGISNULL(1))
        PG_RETURN_BYTEA_P(state);

    value = PG_GETARG_FLOAT8(1);


    if (state == NULL)
    {
        state = MemoryContextAllocZero(agg_context, sizeof(State));
        state->d.float8_values = MemoryContextAllocZero(agg_context, 1024 * sizeof(float8));
        state->alloc_len = 1024;
        state->next_alloc_len = 2048;
        state->nelems = 0;
    }
    else if (state->nelems >= state->alloc_len)
    {
        MemoryContext   old_context = MemoryContextSwitchTo(agg_context);
        int             newlen = state->next_alloc_len;

        state->next_alloc_len += state->alloc_len;
        state->alloc_len = newlen;
        state->d.float4_values = repalloc(state->d.float8_values,
                                          state->alloc_len * sizeof(float8));
        MemoryContextSwitchTo(old_context);
    }

    /* elog(INFO, "Accum value (%d) %2.2f", state->nelems, value); */

    state->d.float8_values[state->nelems++] = value;

    PG_RETURN_BYTEA_P(state);
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;
    State          *state = (State*)(PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_finalfn called in non-aggregate context");

    if (state == NULL)
        PG_RETURN_NULL();

    qsort(state->d.float8_values, state->nelems, sizeof(float8), float8_cmp);

    PG_RETURN_FLOAT8(state->d.float8_values[state->nelems / 2 + 1 - 1]);
}

int
float8_cmp(const void *_a, const void *_b)
{
    float8 a = *((float8 *) _a);
    float8 b = *((float8 *) _b);

    if (isnan(a))
    {
        if (isnan(b))
            return 0;
        else
            return 1;
    }
    else if (isnan(b))
    {
        return -1;
    }
    else
    {
        if (a > b)
            return 1;
        else if (a < b)
            return -1;
        else
            return 0;
    }
}
