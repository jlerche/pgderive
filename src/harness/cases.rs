use crate::transaction::Operation;

type Case = (String, usize, Option<Operation>);

pub(super) fn transactions(schema: &str) -> [Case; 4] {
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
    ]
}
