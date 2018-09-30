CREATE OR REPLACE FUNCTION _median_transfn(state internal, double precision)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, double precision)
RETURNS double precision
AS 'MODULE_PATHNAME', 'median_finalfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (double precision);
CREATE AGGREGATE median (double precision)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_finalfn,
    finalfunc_extra
);
