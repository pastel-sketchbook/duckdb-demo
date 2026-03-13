"""String functions & regex — text processing in SQL.

Covers LIKE/ILIKE, regexp_matches, regexp_extract, regexp_replace,
string_split, concat, and common string manipulation functions.

Run with:
    uv run python examples/34_strings_and_regex.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# -- Setup: create a sample table with text data to manipulate -----------------
conn.execute(
    """
    CREATE TABLE contacts AS
    SELECT * FROM (VALUES
        (1, 'Alice Johnson',   'alice.johnson@example.com',  '(555) 123-4567', 'New York, NY'),
        (2, 'Bob Smith',       'bob_smith@work.org',         '555.987.6543',   'Los Angeles, CA'),
        (3, 'Charlie Brown',   'CHARLIE@EXAMPLE.COM',        '555-111-2222',   'Chicago, IL'),
        (4, 'Diana Prince',    'diana.prince@heroes.net',    '(555) 000-0001', 'Washington, DC'),
        (5, 'Eve Davis',       'eve123@test.io',             '555 444 3333',   'San Francisco, CA'),
        (6, 'Frank O''Brien',  'frank.obrien@example.com',   '555-777-8888',   'Boston, MA')
    ) AS t(id, name, email, phone, city)
    """
)

print("=== Sample contacts ===")
conn.sql("SELECT * FROM contacts").show()

# -- 1. LIKE / ILIKE — basic pattern matching ---------------------------------
# % = any sequence of chars, _ = exactly one char.
# ILIKE is case-insensitive (DuckDB extension).
print("=== LIKE: emails ending in .com ===")
conn.sql("SELECT name, email FROM contacts WHERE email LIKE '%.com'").show()

print("=== ILIKE: case-insensitive search for 'charlie' ===")
conn.sql("SELECT name, email FROM contacts WHERE email ILIKE '%charlie%'").show()

# -- 2. CONCAT / || — joining strings together --------------------------------
# Both forms produce the same result. concat() handles NULLs gracefully.
print("=== String concatenation ===")
conn.sql(
    """
    SELECT
        name,
        'Contact: ' || name || ' <' || email || '>'    AS formatted_v1,
        CONCAT('Contact: ', name, ' <', email, '>')    AS formatted_v2
    FROM contacts
    LIMIT 3
    """
).show()

# -- 3. UPPER / LOWER / INITCAP — case conversion -----------------------------
print("=== Case conversion ===")
conn.sql(
    """
    SELECT
        name,
        UPPER(name)    AS upper_name,
        LOWER(name)    AS lower_name,
        LOWER(email)   AS lower_email
    FROM contacts
    LIMIT 3
    """
).show()

# -- 4. LENGTH / TRIM / LPAD / RPAD — measuring and padding -------------------
print("=== Length, trim, and padding ===")
conn.sql(
    """
    SELECT
        name,
        LENGTH(name)             AS name_len,
        LPAD(CAST(id AS VARCHAR), 5, '0') AS padded_id,
        TRIM('  hello  ')       AS trimmed
    FROM contacts
    LIMIT 3
    """
).show()

# -- 5. SUBSTRING / LEFT / RIGHT — extracting parts ---------------------------
print("=== Substring extraction ===")
conn.sql(
    """
    SELECT
        email,
        -- Extract everything before the @
        SUBSTRING(email, 1, POSITION('@' IN email) - 1) AS local_part,
        -- Extract the domain
        SUBSTRING(email FROM POSITION('@' IN email) + 1) AS domain,
        LEFT(name, 1)  AS first_initial,
        RIGHT(email, 3) AS last_3_chars
    FROM contacts
    """
).show()

# -- 6. REPLACE / TRANSLATE — character substitution ---------------------------
print("=== REPLACE: normalize phone numbers ===")
conn.sql(
    """
    SELECT
        phone AS original,
        -- Strip all non-digit characters
        REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '')
            AS digits_only_manual,
        REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS digits_only_regex
    FROM contacts
    """
).show()

# -- 7. STRING_SPLIT — split a string into an array ---------------------------
# Returns a LIST. Use list indexing or unnest to access elements.
print("=== STRING_SPLIT: parse city and state ===")
conn.sql(
    """
    SELECT
        city AS original,
        STRING_SPLIT(city, ', ')        AS parts,
        STRING_SPLIT(city, ', ')[1]     AS city_name,
        STRING_SPLIT(city, ', ')[2]     AS state_code
    FROM contacts
    """
).show()

# -- 8. REGEXP_MATCHES — test if a string matches a pattern -------------------
# Returns TRUE/FALSE. Useful in WHERE clauses.
print("=== REGEXP_MATCHES: find emails with digits ===")
conn.sql(
    """
    SELECT name, email
    FROM contacts
    WHERE REGEXP_MATCHES(email, '[0-9]')
    """
).show()

# -- 9. REGEXP_EXTRACT — pull matching groups from a string --------------------
# The second argument is the pattern, the third is the group number (0=whole match).
print("=== REGEXP_EXTRACT: extract domain parts ===")
conn.sql(
    """
    SELECT
        email,
        REGEXP_EXTRACT(email, '@(.+)\\.(.+)', 1) AS domain_name,
        REGEXP_EXTRACT(email, '@(.+)\\.(.+)', 2) AS tld
    FROM contacts
    """
).show()

# -- 10. REGEXP_REPLACE — pattern-based substitution --------------------------
print("=== REGEXP_REPLACE: format phone as (NNN) NNN-NNNN ===")
conn.sql(
    """
    SELECT
        phone AS original,
        REGEXP_REPLACE(
            REGEXP_REPLACE(phone, '[^0-9]', '', 'g'),  -- strip non-digits first
            '(\\d{3})(\\d{3})(\\d{4})',                 -- capture groups
            '(\\1) \\2-\\3'                             -- reformat
        ) AS formatted
    FROM contacts
    """
).show()

# -- 11. STRING_AGG — aggregate strings into a delimited list ------------------
# The equivalent of GROUP_CONCAT in MySQL or LISTAGG in Oracle.
print("=== STRING_AGG: list all emails per state ===")
conn.sql(
    """
    SELECT
        STRING_SPLIT(city, ', ')[2] AS state,
        STRING_AGG(name, ', ' ORDER BY name) AS contacts_list
    FROM contacts
    GROUP BY state
    ORDER BY state
    """
).show()

conn.close()
