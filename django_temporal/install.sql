

-- needed for exclusion indexes
CREATE EXTENSION btree_gist ;



CREATE OR REPLACE FUNCTION is_empty(tstzrange) RETURNS boolean AS $$
DECLARE retval boolean;
    BEGIN
        SELECT lower_inc($1) = false
            AND upper_inc($1) = false
            AND lower($1) = upper($1)
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;


CREATE OR REPLACE FUNCTION prior(tstzrange) RETURNS timestamptz AS $$
DECLARE retval timestamptz;
    BEGIN
        SELECT
            CASE
                WHEN lower_inf($1) THEN
                -- error condition
                    null
                WHEN lower_inc($1) THEN
                    lower($1) - interval '1 microsecond'
                ELSE
                    lower($1)
            END
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION prior(daterange) RETURNS date AS $$
DECLARE retval date;
    BEGIN
        SELECT
            CASE
                WHEN lower_inf($1) THEN
                -- error condition
                    null
                WHEN lower_inc($1) THEN
                    lower($1) - interval '1 day'
                ELSE
                    lower($1)
            END
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION first(tstzrange) RETURNS timestamptz AS $$
DECLARE retval timestamptz;
    BEGIN
        SELECT
            CASE
                WHEN lower_inf($1) THEN
                -- error condition
                    null
                WHEN lower_inc($1) THEN
                    lower($1)
                ELSE
                    lower($1) + interval '1 microsecond'
            END
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION last(tstzrange) RETURNS timestamptz AS $$
DECLARE retval timestamptz;
    BEGIN
        SELECT
            CASE
                WHEN upper_inf($1) THEN
                -- error condition
                    null
                WHEN upper_inc($1) THEN
                    upper($1)
                ELSE
                    upper($1) - interval '1 microsecond'
            END
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION next(tstzrange) RETURNS timestamptz AS $$
DECLARE retval timestamptz;
    BEGIN
        SELECT
            CASE
                WHEN upper_inf($1) THEN
                -- error condition
                    null
                WHEN upper_inc($1) THEN
                    upper($1) + interval '1 microsecond'
                ELSE
                    upper($1)
            END
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION next(daterange) RETURNS date AS $$
DECLARE retval date;
    BEGIN
        SELECT
            CASE
                WHEN upper_inf($1) THEN
                -- error condition
                    null
                WHEN upper_inc($1) THEN
                    upper($1) + interval '1 day'
                ELSE
                    upper($1)
            END
        INTO retval;
        RETURN retval;
    END
$$ LANGUAGE plpgsql
IMMUTABLE;
