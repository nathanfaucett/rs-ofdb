use ofdb_sql::Uuid;
use ofdb_test::{ExpectedError, Row, TestCase, TestSuite, Value};

fn uuid(s: &str) -> Value {
    Value::Uuid(Uuid::parse_str(s).unwrap())
}

/// Full CRUD lifecycle: column-list insert with defaults, RETURNING,
/// generated UUID keys, UPDATE, and DELETE.
pub fn case_crud_lifecycle() -> TestCase {
    TestCase::builder("crud_lifecycle")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'unknown')"])
        .step_expect_query(
            0,
            "INSERT INTO users (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)) RETURNING id, name",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc"),
                Value::from("unknown"),
            ])],
        )
        .step(0, "INSERT INTO users (name) VALUES ('Ada')")
        .step(0, "UPDATE users SET name = 'Lin' WHERE name = 'Ada'")
        .step(0, "DELETE FROM users WHERE name = 'unknown'")
        .expect_query(
            "SELECT name FROM users WHERE id IS NOT NULL",
            vec![Row::from(["Lin"])],
        )
        .build()
}

/// Filtering and projection: wildcard and column projection, range
/// comparison, NULL predicates, and boolean composition.
pub fn case_filtering_and_projection() -> TestCase {
    TestCase::builder("filtering_and_projection")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, active BOOLEAN)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', TRUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL, FALSE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin', TRUE)",
        ])
        .expect_query(
            "SELECT * FROM users WHERE name = 'Ada'",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc"),
                Value::from("Ada"),
                Value::Bool(true),
            ])],
        )
        .expect_query(
            "SELECT id, name FROM users WHERE id >= CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID)",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abe"),
                Value::from("Lin"),
            ])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NULL",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abd")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE active = TRUE AND name = 'Lin'",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abe")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NOT NULL AND active = FALSE",
            vec![],
        )
        .build()
}

/// Constraints and indexes: inline UNIQUE enforcement plus index
/// create/drop/recreate over existing rows.
pub fn case_constraints_and_indexes() -> TestCase {
    TestCase::builder("constraints_and_indexes")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
            "CREATE INDEX users_email ON users (email)",
            "DROP INDEX users_email",
            "CREATE INDEX users_email ON users (email)",
        ])
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'ada@example.com')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query(
            "SELECT email FROM users",
            vec![Row::from(["ada@example.com"])],
        )
        .build()
}

/// Schema evolution: ALTER TABLE ADD COLUMN with a default materializes
/// the value on existing rows.
pub fn case_schema_evolution() -> TestCase {
    TestCase::builder("schema_evolution")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'",
            "CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY)",
            "INSERT INTO users (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID))",
        ])
        .expect_query(
            "SELECT role FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
            vec![Row::from(["member"])],
        )
        .expect_query(
            "SELECT role FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)",
            vec![Row::from(["member"])],
        )
        .build()
}

/// A failed statement batch is atomic: nothing in the batch is applied.
pub fn case_atomic_batch() -> TestCase {
    TestCase::builder("atomic_batch")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        ])
        .step_failing(
            0,
            "UPDATE users SET name = 'Lin' WHERE name = 'Ada'; INSERT INTO users VALUES (NULL, 'Bad')",
            ExpectedError::TypeMismatch,
        )
        .expect_query("SELECT name FROM users", vec![Row::from(["Ada"])])
        .step_failing(
            0,
            "CREATE TABLE missing (id UUID PRIMARY KEY); INSERT INTO missing VALUES (NULL)",
            ExpectedError::TypeMismatch,
        )
        .step_failing(0, "SELECT * FROM missing", ExpectedError::TableNotFound)
        .build()
}

/// The error categories real users hit: syntax, missing table, missing
/// column, type mismatch, and constraint violation — none mutate state.
pub fn case_common_errors() -> TestCase {
    TestCase::builder("common_errors")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step_failing(0, "NOT A VALID SQL STATEMENT", ExpectedError::SyntaxError)
        .step_failing(
            0,
            "INSERT INTO nonexistent_table VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'A')",
            ExpectedError::TableNotFound,
        )
        .step_failing(
            0,
            "SELECT nonexistent_column FROM users",
            ExpectedError::ColumnNotFound,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES ('not-a-valid-uuid', 'B')",
            ExpectedError::TypeMismatch,
        )
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Lin')",
            ExpectedError::ConstraintViolation,
        )
        .step_failing(
            0,
            "CREATE TABLE invalid_schema (id UUID PRIMARY KEY, created DATE)",
            ExpectedError::SyntaxError,
        )
        .step_failing(0, "SELECT * FROM invalid_schema", ExpectedError::TableNotFound)
        .expect_query("SELECT name FROM users", vec![Row::from(["Ada"])])
        .build()
}

/// Keyed mutations only affect the matching row, and no-match predicates are harmless.
pub fn case_keyed_crud_boundaries() -> TestCase {
    TestCase::builder("keyed_crud_boundaries")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, alias TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin', NULL)",
        ])
        .step(
                    0,
                    "UPDATE users SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
                )
        .step(
                    0,
                    "UPDATE users SET alias = name WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)",
                )
        .step(0, "UPDATE users SET name = 'Wrong' WHERE name = 'missing'")
        .step(0, "DELETE FROM users WHERE name = 'missing'")
        .step(
                    0,
                    "DELETE FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
                )
        .expect_query("SELECT name FROM users WHERE name = 'Grace'", vec![])
        .expect_query("SELECT name FROM users WHERE name = 'Lin'", vec![Row::from(["Lin"])])
        .expect_query("SELECT name FROM users", vec![Row::from(["Lin"])])
        .expect_query("SELECT alias FROM users", vec![Row::from(["Lin"])])
        .build()
}

/// Predicate operators select the expected rows from a mixed dataset.
pub fn case_mixed_predicates() -> TestCase {
    TestCase::builder("mixed_predicates")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, age INTEGER)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', 20)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL, 30)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin', 40)",
        ])
        .expect_query("SELECT name FROM users WHERE name <> 'Ada' OR name IS NULL", vec![Row::new(vec![Value::Null]), Row::from(["Lin"])])
        .expect_query("SELECT name FROM users WHERE (age >= 20 AND age < 40) OR name = 'Lin'", vec![Row::from(["Ada"]), Row::new(vec![Value::Null]), Row::from(["Lin"])])
        .expect_query("SELECT name FROM users WHERE age <= 20", vec![Row::from(["Ada"])])
        .expect_query("SELECT name FROM users WHERE age > 40", vec![])
        .expect_query("SELECT name FROM users WHERE name IS NOT NULL AND age >= 40", vec![Row::from(["Lin"])])
        .build()
}

/// UPDATE and DELETE predicates affect every matching row and spare others.
pub fn case_multirow_mutations() -> TestCase {
    TestCase::builder("multirow_mutations")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, alias TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin', NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Grace', NULL)",
        ])
        .step(0, "UPDATE users SET alias = name WHERE name IS NOT NULL")
        .step(0, "DELETE FROM users WHERE name <> 'Lin'")
        .expect_query("SELECT name FROM users", vec![Row::from(["Lin"])])
        .expect_query("SELECT alias FROM users", vec![Row::from(["Lin"])])
        .build()
}

/// COUNT(*) and COUNT(column) include empty and non-NULL row semantics.
pub fn case_counts() -> TestCase {
    TestCase::builder("counts")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step_expect_query(
            0,
            "SELECT COUNT(*) FROM users",
            vec![Row::from([0_i64])],
        )
        .step(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')")
        .step(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)")
        .expect_query("SELECT COUNT(*) FROM users", vec![Row::from([2_i64])])
        .expect_query("SELECT COUNT(name) FROM users", vec![Row::from([1_i64])])
        .build()
}

/// Multi-row INSERT inserts each tuple and rolls back all rows on a later failure.
pub fn case_multirow_insert_atomicity() -> TestCase {
    TestCase::builder("multirow_insert_atomicity")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step(
            0,
            "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada'), (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        )
        .step_failing(
            0,
            "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Grace'), (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Duplicate')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query(
            "SELECT name FROM users ORDER BY name",
            vec![Row::from(["Ada"]), Row::from(["Lin"])],
        )
        .expect_query("SELECT name FROM users WHERE name = 'Grace'", vec![])
        .build()
}

/// A duplicate UUID primary key is ignored without changing the existing row.
pub fn case_insert_on_conflict_do_nothing() -> TestCase {
    TestCase::builder("insert_on_conflict_do_nothing")
        .setup([
            "CREATE TABLE users (uuid_primary_key UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        ])
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Lin') ON CONFLICT (uuid_primary_key) DO NOTHING",
        )
        .expect_query("SELECT name FROM users", vec![Row::from(["Ada"])])
        .build()
}

/// A primary-key conflict updates only the requested columns of the existing row.
pub fn case_insert_on_conflict_do_update() -> TestCase {
    TestCase::builder("insert_on_conflict_do_update")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, active BOOLEAN)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', TRUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin', FALSE)",
        ])
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Grace', FALSE) ON CONFLICT (id) DO UPDATE SET name = 'Grace'",
        )

        .step(
            0,
            "INSERT INTO users (id, name, active) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'First', TRUE), (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Second', FALSE) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
        )
        .expect_query(
            "SELECT name FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
            vec![Row::from(["Second"])],
        )
        .expect_query(
            "SELECT name, active FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
            vec![Row::new(vec![Value::from("Second"), Value::Bool(true)])],
        )
        .expect_query(
            "SELECT name, active FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)",
            vec![Row::new(vec![Value::from("Lin"), Value::Bool(false)])],
        )
        .build()
}

/// Ordered lists support direction, NULL values, limits, and offsets.
pub fn case_ordered_lists_and_pagination() -> TestCase {
    TestCase::builder("ordered_lists_and_pagination")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), 'Lin')",
        ])
        .expect_query(
            "SELECT name FROM users ORDER BY name ASC LIMIT 3 OFFSET 1",
            vec![Row::from(["Ada"]), Row::from(["Lin"]), Row::from(["Lin"])],
        )
        .expect_query(
            "SELECT name FROM users ORDER BY name DESC LIMIT 2",
            vec![Row::from(["Lin"]), Row::from(["Lin"])],
        )
        .expect_query(
            "SELECT name FROM users ORDER BY name ASC LIMIT 1",
            vec![Row::new(vec![Value::Null])],
        )
        .expect_query("SELECT name FROM users ORDER BY name LIMIT 0", vec![])
        .expect_query(
            "SELECT name FROM users ORDER BY name LIMIT 2 OFFSET 20",
            vec![],
        )
        .build()
}

/// Explicit NULL and DEFAULT are distinct from omitted columns.
pub fn case_insert_defaults_and_null() -> TestCase {
    TestCase::builder("insert_defaults_and_null")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'unknown')"])
        .step(0, "INSERT INTO users (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))")
        .step(0, "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)")
        .step(0, "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), DEFAULT)")

        .expect_query("SELECT name FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)", vec![Row::from(["unknown"])])
        .expect_query("SELECT name FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)", vec![Row::new(vec![Value::Null])])
        .expect_query("SELECT name FROM users WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID)", vec![Row::from(["unknown"])])
        .build()
}

/// Unique indexes allow multiple NULL values while enforcing non-NULL values.
pub fn case_unique_null_values() -> TestCase {
    TestCase::builder("unique_null_values")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT)",
            "CREATE UNIQUE INDEX users_email ON users (email)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)",
        ])
        .expect_query(
            "SELECT email FROM users WHERE email IS NULL",
            vec![Row::new(vec![Value::Null]), Row::new(vec![Value::Null])],
        )
        .build()
}

/// Numeric, boolean, text, blob, and UUID values round-trip through SQL.
pub fn case_supported_type_roundtrips() -> TestCase {
    TestCase::builder("supported_type_roundtrips")
        .setup([
            "CREATE TABLE typed (id UUID PRIMARY KEY, count INTEGER, ratio FLOAT, active BOOLEAN, label TEXT, data BLOB)",
            "INSERT INTO typed VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 42, 1.5, TRUE, 'sample', X'0102')",
        ])
        .expect_query(
            "SELECT count, ratio, active, label, data FROM typed WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
            vec![Row::new(vec![Value::Integer(42), Value::Float(1.5), Value::Bool(true), Value::from("sample"), Value::Blob(vec![1, 2])])],
        )
        .build()
}

/// UNIQUE indexes remain enforced when rows are updated.
pub fn case_unique_update_integrity() -> TestCase {
    TestCase::builder("unique_update_integrity")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT)",
            "CREATE UNIQUE INDEX users_email ON users (email)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'lin@example.com')",
        ])
        .step_failing(0, "UPDATE users SET email = 'ada@example.com' WHERE email = 'lin@example.com'", ExpectedError::ConstraintViolation)
        .expect_query("SELECT email FROM users WHERE email = 'ada@example.com'", vec![Row::from(["ada@example.com"])])
        .expect_query("SELECT email FROM users WHERE email = 'lin@example.com'", vec![Row::from(["lin@example.com"])])
        .build()
}

pub fn case_inner_and_left_joins() -> TestCase {
    TestCase::builder("inner_and_left_joins")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders (id UUID PRIMARY KEY, user_id TEXT, item TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
            "INSERT INTO orders VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Ada', 'book')",
        ])
        .expect_query(
            "SELECT users.name, orders.item FROM users INNER JOIN orders ON users.name = orders.user_id",
            vec![Row::from(["Ada", "book"])],
        )
        .expect_query(
            "SELECT users.name, orders.item FROM users LEFT JOIN orders ON users.name = orders.user_id",
            vec![Row::from(["Ada", "book"]), Row::new(vec![Value::from("Lin"), Value::Null])],
        )
        .build()
}

/// DISTINCT collapses duplicate projected rows before pagination.
pub fn case_distinct_projection() -> TestCase {
    TestCase::builder("distinct_projection")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')",
        ])
        .expect_query(
            "SELECT DISTINCT name FROM users ORDER BY name LIMIT 2",
            vec![Row::from(["Ada"]), Row::from(["Lin"])],
        )
        .build()
}

/// IN and NOT IN support literal lists, including SQL NULL propagation.
pub fn case_in_list_predicates() -> TestCase {
    TestCase::builder("in_list_predicates")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), NULL)",
        ])
        .expect_query(
            "SELECT name FROM users WHERE name IN ('Ada', 'Lin') ORDER BY name",
            vec![Row::from(["Ada"]), Row::from(["Ada"]), Row::from(["Lin"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name NOT IN ('Ada') ORDER BY name",
            vec![Row::from(["Lin"])],
        )
        .build()
}

/// LIKE supports case-sensitive `%` and `_` wildcards.
pub fn case_like_predicates() -> TestCase {
    TestCase::builder("like_predicates")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Al')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), NULL)",
        ])
        .expect_query(
            "SELECT name FROM users WHERE name LIKE 'A_a'",
            vec![Row::from(["Ada"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name LIKE 'A%' ORDER BY name",
            vec![Row::from(["Ada"]), Row::from(["Al"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name NOT LIKE 'A%'",
            vec![Row::from(["Lin"])],
        )
        .build()
}

/// DROP TABLE removes the table and IF EXISTS is harmless for absent tables.
pub fn case_drop_table() -> TestCase {
    TestCase::builder("drop_table")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        ])
        .step(0, "DROP TABLE users")
        .step(0, "DROP TABLE IF EXISTS users")
        .step_failing(0, "SELECT name FROM users", ExpectedError::TableNotFound)
        .build()
}

/// UPDATE and DELETE RETURNING yield the changed/deleted row values.
pub fn case_mutation_returning() -> TestCase {
    TestCase::builder("mutation_returning")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        ])
        .step_expect_query(
            0,
            "UPDATE users SET name = 'Grace' WHERE name = 'Ada' RETURNING name",
            vec![Row::from(["Grace"])],
        )
        .step_expect_query(
            0,
            "DELETE FROM users WHERE name = 'Lin' RETURNING name",
            vec![Row::from(["Lin"])],
        )
        .expect_query("SELECT name FROM users", vec![Row::from(["Grace"])])
        .build()
}

/// GROUP BY supports one simple column with COUNT(*) or COUNT(column).
pub fn case_grouped_counts() -> TestCase {
    TestCase::builder("grouped_counts")
        .setup([
            "CREATE TABLE people (id UUID PRIMARY KEY, team TEXT, score INTEGER)",
            "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'alpha', 3)",
            "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'alpha', NULL)",
            "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'beta', NULL)",
        ])
        .expect_query(
            "SELECT team, COUNT(*) FROM people GROUP BY team ORDER BY team",
            vec![
                Row::new(vec![Value::from("alpha"), Value::Integer(2)]),
                Row::new(vec![Value::from("beta"), Value::Integer(1)]),
            ],
        )
        .expect_query(
            "SELECT team, COUNT(score) FROM people GROUP BY team ORDER BY team",
            vec![
                Row::new(vec![Value::from("alpha"), Value::Integer(1)]),
                Row::new(vec![Value::from("beta"), Value::Integer(0)]),
            ],
        )
        .expect_query(
            "SELECT team, COUNT(*) FROM people GROUP BY team HAVING COUNT(*) > 1 ORDER BY team",
            vec![Row::new(vec![Value::from("alpha"), Value::Integer(2)])],
        )
        .expect_query(
            "SELECT team, COUNT(*) FROM people GROUP BY team HAVING COUNT(*) >= 1 ORDER BY team",
            vec![
                Row::new(vec![Value::from("alpha"), Value::Integer(2)]),
                Row::new(vec![Value::from("beta"), Value::Integer(1)]),
            ],
        )
        .expect_query(
            "SELECT team || '!' AS decorated FROM people ORDER BY team",
            vec![Row::from(["alpha!"]), Row::from(["alpha!"]), Row::from(["beta!"])],
        )
        .build()
}

pub fn case_in_subquery() -> TestCase {
    TestCase::builder("in_subquery")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "CREATE TABLE selected_names (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
            "INSERT INTO selected_names VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Ada')",
            "INSERT INTO selected_names VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abf' AS UUID), 'Lin')",
        ])
        .expect_query(
            "SELECT name FROM users WHERE name IN (SELECT name FROM selected_names)",
            vec![Row::from(["Ada"]), Row::from(["Lin"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name NOT IN (SELECT name FROM selected_names WHERE name = 'Ada')",
            vec![Row::from(["Lin"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name IN (SELECT name FROM selected_names WHERE name = 'Ada')",
            vec![Row::from(["Ada"])],
        )
        .expect_query(
            "WITH selected AS (SELECT name FROM users WHERE name = 'Ada') SELECT name FROM selected",
            vec![Row::from(["Ada"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name IN (SELECT name FROM selected_names WHERE name IN (SELECT name FROM selected_names WHERE name = 'Ada'))",
            vec![Row::from(["Ada"])],
        )
        .build()
}

/// Contract: SQL the engine does not support yet is rejected without
/// mutating state. Expected to change as support lands.
pub fn case_unsupported_sql_is_rejected() -> TestCase {
    TestCase::builder("unsupported_sql_is_rejected")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com', 'Ada')",
        ])
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-123456789abd' AS UUID), 'ada@example.com', 'Lin') ON CONFLICT (email) DO NOTHING",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-123456789abd' AS UUID), 'lin@example.com', 'Lin') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.name <> EXCLUDED.name",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT DISTINCT ON (name) name FROM users",
            ExpectedError::SyntaxError,
        )

        .step_failing(
            0,
            "WITH RECURSIVE selected AS (SELECT name FROM users) SELECT name FROM selected",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "WITH first_names AS (SELECT name FROM users), second_names AS (SELECT name FROM users) SELECT name FROM first_names",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT (SELECT name FROM users) AS name",
            ExpectedError::SyntaxError,
        )
        .step_failing(0, "BEGIN", ExpectedError::SyntaxError)
        .step_failing(0, "COMMIT", ExpectedError::SyntaxError)
        .step_failing(0, "PRAGMA foreign_keys = ON", ExpectedError::SyntaxError)
        .step_failing(
            0,
            "CREATE TABLE sequence (id INTEGER PRIMARY KEY AUTOINCREMENT)",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "CREATE TABLE child (id UUID PRIMARY KEY, parent_id UUID REFERENCES users(id))",
            ExpectedError::SyntaxError,
        )
        .step_failing(0, "SELECT * FROM child", ExpectedError::TableNotFound)
        .step_failing(0, "SELECT rowid FROM users", ExpectedError::ColumnNotFound)
        .step_failing(
            0,
            "SELECT name FROM users GROUP BY name",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT name FROM users GROUP BY name HAVING COUNT(*) = 1",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT name || '!' FROM users",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT id + 1 AS adjusted FROM users",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT * FROM users, users AS other",
            ExpectedError::SyntaxError,
        )
        .expect_query("SELECT name FROM users", vec![Row::from(["Ada"])])
        .expect_query("SELECT email FROM users", vec![Row::from(["ada@example.com"])])
        .build()
}

/// The 90% single-node SQL surface.
pub fn sql_suite() -> TestSuite {
    TestSuite::new("sql_surface_suite")
        .with_case(case_crud_lifecycle())
        .with_case(case_filtering_and_projection())
        .with_case(case_constraints_and_indexes())
        .with_case(case_schema_evolution())
        .with_case(case_atomic_batch())
        .with_case(case_common_errors())
        .with_case(case_keyed_crud_boundaries())
        .with_case(case_mixed_predicates())
        .with_case(case_insert_defaults_and_null())
        .with_case(case_ordered_lists_and_pagination())
        .with_case(case_multirow_insert_atomicity())
        .with_case(case_insert_on_conflict_do_nothing())
        .with_case(case_insert_on_conflict_do_update())
        .with_case(case_counts())
        .with_case(case_multirow_mutations())
        .with_case(case_supported_type_roundtrips())
        .with_case(case_unique_update_integrity())
        .with_case(case_unique_null_values())
        .with_case(case_inner_and_left_joins())
        .with_case(case_distinct_projection())
        .with_case(case_in_list_predicates())
        .with_case(case_in_subquery())
        .with_case(case_like_predicates())
        .with_case(case_drop_table())
        .with_case(case_mutation_returning())
        .with_case(case_grouped_counts())
}

/// Contract tests for SQL the engine does not support yet (non-default).
pub fn sql_limits_suite() -> TestSuite {
    TestSuite::new("sql_limits_suite").with_case(case_unsupported_sql_is_rejected())
}
