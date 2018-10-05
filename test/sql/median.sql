CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;


-- Test even number of float values
CREATE TABLE floatvals (val float(10));
INSERT INTO floatvals VALUES
       (2.0),
       (4.0);
SELECT median(val) FROM floatvals;

-- Test odd number of float values
INSERT INTO floatvals VALUES (3.5);
SELECT median(val) FROM floatvals;

-- Test float NaN
INSERT INTO floatvals VALUES ('NaN');
SELECT median(val) FROM floatvals;

DELETE FROM floatvals;
INSERT INTO floatvals VALUES
       ('NaN'),
       (1.0);
SELECT median(val) FROM floatvals;

-- Test window aggregates
SELECT median(color) OVER (ORDER BY color DESC) FROM textvals;

SELECT median(val) OVER (ORDER BY val DESC) FROM textvals;

SELECT median(val)
    OVER (ORDER BY val ROWS between CURRENT ROW AND 1 FOLLOWING)
    FROM intvals;

SELECT median(color)
    OVER (ORDER BY color ROWS between CURRENT ROW AND 1 FOLLOWING)
    FROM intvals;
