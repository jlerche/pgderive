use crate::transaction::Operation;

pub(super) const COUNT: usize = 10;

type Case = (String, usize, Option<Operation>);

pub(super) fn transactions(schema: &str) -> [Case; COUNT] {
    [
        (
            format!(
                "BEGIN;
            INSERT INTO {schema}.person VALUES(1,'Alice');
            INSERT INTO {schema}.auction VALUES(1,1,10);
            INSERT INTO {schema}.bid SELECT n,1,1,100+n FROM generate_series(1,10) n; COMMIT;"
            ),
            12,
            Some(Operation::Insert),
        ),
        (
            format!(
                "BEGIN; INSERT INTO {schema}.person VALUES(99,'rolled back'); ROLLBACK;
            BEGIN; UPDATE {schema}.person SET name=NULL WHERE id=1;
            UPDATE {schema}.auction SET category=20 WHERE id=1;
            UPDATE {schema}.bid SET price=price+100; COMMIT;"
            ),
            12,
            Some(Operation::Update),
        ),
        (
            format!("BEGIN; DELETE FROM {schema}.bid WHERE id%2=0; COMMIT;"),
            5,
            Some(Operation::Delete),
        ),
        (
            format!(
                "BEGIN;
            INSERT INTO {schema}.bid VALUES(11,1,1,999);
            DELETE FROM {schema}.bid WHERE id=11;
            UPDATE {schema}.bid SET price=price WHERE id=1;
            UPDATE {schema}.person SET name='temporary' WHERE id=1;
            UPDATE {schema}.person SET name=NULL WHERE id=1; COMMIT;"
            ),
            5,
            None,
        ),
        (
            format!(
                "BEGIN; UPDATE {schema}.auction SET category=NULL WHERE id=1;
            UPDATE {schema}.bid SET price=NULL WHERE id=1; COMMIT;"
            ),
            2,
            Some(Operation::Update),
        ),
        (format!("UPDATE {schema}.bid SET price=NULL WHERE id=5"), 1, Some(Operation::Update)),
        (
            format!(
                "BEGIN; UPDATE {schema}.bid SET price=250 WHERE id=5;
            UPDATE {schema}.auction SET category=30 WHERE id=1; COMMIT;"
            ),
            2,
            Some(Operation::Update),
        ),
        (format!("UPDATE {schema}.bid SET price=0 WHERE price>=205"), 3, Some(Operation::Update)),
        (
            format!(
                "BEGIN; UPDATE {schema}.auction SET category=20 WHERE id=1;
            UPDATE {schema}.bid SET price=300 WHERE id=1;
            INSERT INTO {schema}.bid VALUES(12,1,NULL,300); COMMIT;"
            ),
            3,
            None,
        ),
        (format!("DELETE FROM {schema}.bid WHERE price>=205"), 2, Some(Operation::Delete)),
    ]
}
