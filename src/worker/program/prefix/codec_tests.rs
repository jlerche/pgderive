//! Exercise actual trie paths/cover against an independent signed-order scan.
use super::codec::{cover, path};
use crate::worker::program::order;
use anyhow::Result;
use num_bigint::BigInt;
use std::collections::BTreeMap;
type Source = Vec<(i64, Option<i64>, i64)>;
type Tree = BTreeMap<(u32, u64), Vec<BigInt>>;
fn contribution(value: Option<i64>, weight: i64) -> Vec<BigInt> {
    vec![
        weight.into(),
        if value.is_some() { weight.into() } else { 0.into() },
        BigInt::from(value.unwrap_or(0)) * weight,
    ]
}
fn add(total: &mut [BigInt], values: &[BigInt]) {
    for (total, value) in total.iter_mut().zip(values) {
        *total += value;
    }
}
fn change(tree: &mut Tree, (position, value, weight): (i64, Option<i64>, i64), sign: i64) {
    let values = contribution(value, weight * sign);
    for node in path(order::biased(position, false)) {
        let total = tree.entry(node).or_insert_with(|| vec![0.into(); 3]);
        add(total, &values);
        if total[0] == 0.into() {
            assert!(total.iter().all(|value| value == &0.into()));
            tree.remove(&node);
        }
    }
}
fn compare(tree: &Tree, source: &Source, target: i64) {
    let mut expected = vec![BigInt::from(0); 3];
    for (position, value, weight) in source {
        if *position <= target {
            add(&mut expected, &contribution(*value, *weight));
        }
    }
    let mut actual = vec![BigInt::from(0); 3];
    for node in cover(order::biased(target, false)) {
        if let Some(values) = tree.get(&node) {
            add(&mut actual, values);
        }
    }
    assert_eq!(actual, expected, "target {target}");
}
#[test]
fn trie_prefixes_match_21000_full_signed_order_scans() -> Result<()> {
    for seed in 0_u64..30 {
        check_seed(seed)?;
    }
    Ok(())
}
fn check_seed(mut random: u64) -> Result<()> {
    let mut source = Source::new();
    let mut tree = Tree::new();
    for step in 0_i64..100 {
        random = random.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let position = match step % 5 {
            0 => i64::MIN,
            1 => -1,
            2 => 0,
            3 => i64::MAX,
            _ => i64::from_be_bytes(random.to_be_bytes()),
        };
        if step % 3 == 0
            && let Some(row) = source.pop()
        {
            change(&mut tree, row, -1);
        } else {
            let value = (step % 4 != 0).then_some(i64::try_from(random % 200)? - 100);
            let row = (position, value, step % 3 + 1);
            source.push(row);
            change(&mut tree, row, 1);
        }
        for target in [
            position,
            i64::MIN,
            -1,
            0,
            1,
            i64::MAX,
            i64::from_be_bytes(random.rotate_left(17).to_be_bytes()),
        ] {
            compare(&tree, &source, target);
        }
    }
    Ok(())
}
