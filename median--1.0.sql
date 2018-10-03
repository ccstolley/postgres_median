CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_invfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_invfn'
LANGUAGE C IMMUTABLE;

/* numeric types coerced to float8's */

CREATE OR REPLACE FUNCTION _median_double_finalfn(state internal, val double precision)
RETURNS double precision
AS 'MODULE_PATHNAME', 'median_double_finalfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (double precision);
CREATE AGGREGATE median (double precision)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_double_finalfn,
    finalfunc_extra,
    msfunc = _median_transfn,
    mstype = internal,
    minvfunc = _median_invfn,
    mfinalfunc = _median_double_finalfn,
    mfinalfunc_extra
);

/* timestamp with timezone */

CREATE OR REPLACE FUNCTION _median_timestamptz_finalfn(state internal, val timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'median_timestamptz_finalfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (timestamptz);
CREATE AGGREGATE median (timestamptz)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_timestamptz_finalfn,
    finalfunc_extra,
    msfunc = _median_transfn,
    mstype = internal,
    minvfunc = _median_invfn,
    mfinalfunc = _median_timestamptz_finalfn,
    mfinalfunc_extra
);


/* strings */

CREATE OR REPLACE FUNCTION _median_text_finalfn(state internal, val text)
RETURNS text
AS 'MODULE_PATHNAME', 'median_text_finalfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (text);
CREATE AGGREGATE median (text)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_text_finalfn,
    finalfunc_extra,
    msfunc = _median_transfn,
    mstype = internal,
    minvfunc = _median_invfn,
    mfinalfunc = _median_text_finalfn,
    mfinalfunc_extra
);

