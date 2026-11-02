use super::*;
use crate::compiler::{
    ColumnRef,
    projection::{OutputColumn, Projected},
    relational::{Binding, Source},
};
use crate::source::{Contract, tests::column};
fn contract() -> Contract {
    let mut native = crate::source::tests::contract();
    for (position, name, oid, nullable) in
        [(2, "category", 23, false), (3, "optional", 23, true), (4, "parent", 20, true)]
    {
        let mut field = column(position, name, oid);
        field.nullable = nullable;
        native.relations[0].columns.push(field);
    }
    let mut other = native.relations[0].clone();
    other.oid = 43;
    other.table = "other".into();
    native.relations.push(other);
    native
}
fn check(sql: &str, names: &[&str], expected: bool) -> Result<()> {
    let native = contract();
    let compiled =
        crate::compiler::compile(sql, &native).map_err(|e| anyhow::anyhow!("{sql}: {e:#}"))?;
    let names = names.iter().map(|s| (*s).into()).collect::<Vec<String>>();
    assert_eq!(
        Analysis::infer(&native, compiled.relational())?.prove_sink(&compiled, &names).is_ok(),
        expected,
        "{sql}: {names:?}"
    );
    Ok(())
}
#[test]
fn source_projection_grouping_aliases_and_nullable_filters() -> Result<()> {
    for (sql, key, accepted) in [
        ("SELECT id FROM public.items", "id", true),
        ("SELECT id AS key FROM public.items WHERE category>0", "key", true),
        ("SELECT category FROM public.items", "category", false),
        ("SELECT id-id AS key FROM public.items", "key", false),
        ("SELECT id,category,COUNT(*) FROM public.items GROUP BY id,category", "id", false),
        ("SELECT optional,COUNT(*) FROM public.items GROUP BY optional", "optional", false),
        (
            "SELECT x.optional FROM (SELECT optional,COUNT(*) FROM public.items GROUP BY optional) x WHERE x.optional IS NOT NULL",
            "optional",
            true,
        ),
        ("SELECT DISTINCT id FROM public.items", "id", true),
        ("SELECT DISTINCT category FROM public.items", "category", true),
    ] {
        check(sql, &[key], accepted)?;
    }
    check(
        "SELECT id,category,COUNT(*) FROM public.items GROUP BY id,category",
        &["category", "id"],
        true,
    )?;
    check("SELECT x.b FROM (SELECT id AS a,id AS b FROM public.items) x", &["b"], true)?;
    Ok(())
}
#[test]
fn joins_require_actual_matching_key_and_distinct_row_occurrences() -> Result<()> {
    let many_one =
        "SELECT a.id AS lid,b.id AS rid FROM public.items a JOIN public.other b ON a.parent=b.id";
    check(many_one, &["lid"], true)?;
    check(many_one, &["rid"], false)?;
    check(many_one, &["lid", "rid"], true)?;
    for sql in [
        "SELECT a.id AS lid,b.id AS rid FROM public.items a JOIN public.other b ON a.category=b.category",
        "SELECT a.id AS lid,b.id AS rid FROM public.items a JOIN public.items b ON a.category=b.category",
        "SELECT a.id AS lid,b.id AS rid FROM public.items a JOIN public.other b ON a.id-a.id=b.id-b.id",
    ] {
        check(sql, &["lid"], false)?;
        check(sql, &["lid", "rid"], true)?;
    }
    check(
        "SELECT a.id AS lid,b.id AS rid FROM public.items a LEFT JOIN public.other b ON a.parent=b.id",
        &["lid"],
        true,
    )?;
    check(
        "SELECT a.id AS lid,b.id AS rid FROM public.items a LEFT JOIN public.other b ON a.category=b.category",
        &["lid"],
        false,
    )?;
    check(
        "SELECT a.id AS lid,b.id AS rid FROM public.items a LEFT JOIN public.other b ON a.parent=b.id",
        &["rid"],
        false,
    )?;
    Ok(())
}
#[test]
fn positional_order_can_use_either_proven_one_to_one_key() -> Result<()> {
    let native = contract();
    crate::compiler::compile(
        "SELECT x.rid,ROW_NUMBER() OVER (ORDER BY x.rid) AS rn FROM (SELECT a.id AS lid,b.id AS rid FROM public.items a JOIN public.other b ON a.id=b.id) x",
        &native,
    )?;
    assert!(crate::compiler::compile("SELECT x.category,ROW_NUMBER() OVER (ORDER BY x.category) AS rn FROM (SELECT a.id AS lid,b.id AS rid,a.category FROM public.items a JOIN public.other b ON a.category=b.category) x",&native).is_err());
    Ok(())
}
fn reference(name: &str, oid: u32) -> ColumnRef {
    ColumnRef { name: name.into(), right: false, oid, nullable: false }
}
#[test]
fn forked_source_lineage_is_not_a_join_value_equivalence() -> Result<()> {
    let native = contract();
    let id = reference("0:id", 20);
    let category = reference("0:category", 23);
    let rebind = |prefix: &str| {
        vec![
            Binding { input: id.clone(), output: reference(&format!("{prefix}id"), 20) },
            Binding { input: category.clone(), output: reference(&format!("{prefix}c"), 23) },
        ]
    };
    let ir = Relational {
        sources: vec![Source { schema: "public".into(), table: "items".into() }],
        projection: Vec::new(),
        nodes: vec![
            Node::Source { id: "s".into(), source: 0 },
            Node::Rebind { id: "l".into(), input: "s".into(), fields: rebind("l") },
            Node::Rebind { id: "r".into(), input: "s".into(), fields: rebind("r") },
            Node::EquijoinKey {
                id: "lk".into(),
                input: "l".into(),
                keys: vec![reference("lc", 23)],
            },
            Node::EquijoinKey {
                id: "rk".into(),
                input: "r".into(),
                keys: vec![reference("rc", 23)],
            },
            Node::Join { id: "j".into(), left: "lk".into(), right: "rk".into() },
            Node::Output { id: "o".into(), input: "j".into() },
        ],
        output: Projected {
            columns: vec![
                OutputColumn { label: "lid".into(), column: reference("lid", 20) },
                OutputColumn { label: "rid".into(), column: reference("rid", 20) },
            ],
        },
    };
    let facts = Analysis::infer(&native, &ir)?;
    assert!(!facts.edge("j")?.unique_rows());
    let mut grouped = ir;
    grouped.nodes.pop();
    let keys = vec![reference("lid", 20), reference("rid", 20)];
    grouped.nodes.extend([
        Node::PartitionBy { id: "gk".into(), input: "j".into(), keys: keys.clone() },
        Node::Partition {
            id: "g".into(),
            input: "gk".into(),
            spec: super::super::partition::Partition {
                mode: Mode::Grouped { keys },
                aggregates: Vec::new(),
            },
        },
        Node::Output { id: "o".into(), input: "g".into() },
    ]);
    let facts = Analysis::infer(&native, &grouped)?;
    assert!(!facts.edge("o")?.proves_key(&["lid".into()]));
    assert!(facts.edge("o")?.proves_key(&["lid".into(), "rid".into()]));
    Ok(())
}
#[test]
fn equality_and_cardinality_are_proven_not_sampled() -> Result<()> {
    let native = contract();
    let mut compiled =
        crate::compiler::compile("SELECT id,COUNT(*) AS n FROM public.items GROUP BY id", &native)?;
    for node in &mut compiled.relational.nodes {
        match node {
            Node::PartitionBy { keys, .. } => keys.clear(),
            Node::Statistics { spec, .. } | Node::Finalize { spec, .. } => {
                spec.mode = Mode::Grouped { keys: vec![] }
            }
            _ => {}
        }
    }
    compiled.relational.output.columns.retain(|c| c.label == "n");
    let facts = Analysis::infer(&native, compiled.relational())?;
    assert_eq!(facts.edge("project")?.maximum_rows(), Some(1));
    let mut text = native.clone();
    text.relations[0].columns[0].oid = 25;
    text.relations[0].columns[0].collation = 100;
    let compiled = crate::compiler::compile("SELECT id FROM public.items", &text)?;
    assert!(
        Analysis::infer(&text, compiled.relational())?
            .prove_sink(&compiled, &["id".into()])
            .is_err()
    );
    let mut numeric = native;
    numeric.relations[0].columns[0].oid = 1700;
    let compiled = crate::compiler::compile("SELECT id FROM public.items", &numeric)?;
    assert!(
        Analysis::infer(&numeric, compiled.relational())?
            .prove_sink(&compiled, &["id".into()])
            .is_err()
    );
    Ok(())
}

#[test]
fn union_all_discards_keys_even_for_the_same_input() -> Result<()> {
    let native = contract();
    let mut compiled = crate::compiler::compile("SELECT id FROM public.items", &native)?;
    let Some(Node::Output { input, .. }) = compiled.relational.nodes.pop() else {
        anyhow::bail!("missing fixture output");
    };
    compiled.relational.nodes.extend([
        Node::Union { id: "u".into(), left: input.clone(), right: input },
        Node::Output { id: "o".into(), input: "u".into() },
    ]);
    let facts = Analysis::infer(&native, compiled.relational())?;
    assert!(!facts.edge("o")?.unique_rows());
    assert!(facts.prove_sink(&compiled, &["id".into()]).is_err());
    Ok(())
}

#[test]
fn expansion_requires_ordinal_and_composite_sources_require_every_component() -> Result<()> {
    let sql = "SELECT a.id,w.n AS ordinal FROM public.items a CROSS JOIN generate_series(0,2) w(n)";
    check(sql, &["id"], false)?;
    check(sql, &["id", "ordinal"], true)?;
    let mut native = contract();
    native.relations[0].columns[1].primary = true;
    for (sql, names, accepted) in [
        ("SELECT id FROM public.items", vec!["id"], false),
        ("SELECT id,category FROM public.items", vec!["id"], false),
        ("SELECT id,category FROM public.items", vec!["id", "category"], true),
    ] {
        let compiled = crate::compiler::compile(sql, &native)?;
        let names = names.into_iter().map(String::from).collect::<Vec<_>>();
        assert_eq!(
            Analysis::infer(&native, compiled.relational())?.prove_sink(&compiled, &names).is_ok(),
            accepted
        );
    }
    Ok(())
}

#[test]
fn navigation_equality_must_match_type_modifier_and_collation() -> Result<()> {
    let native = contract();
    let mut left = Analysis::source_relation(&native.relations[0], 0)?;
    let mut right = Analysis::source_relation(&native.relations[1], 1)?;
    left.navigation = vec!["0:parent".into()];
    right.navigation = vec!["1:id".into()];
    for mismatch in 0..3 {
        let mut changed = right.clone();
        let column = changed
            .columns
            .get_mut("1:id")
            .ok_or_else(|| anyhow::anyhow!("missing fixture field"))?;
        match mismatch {
            0 => column.reference.oid = 23,
            1 => column.modifier = 1,
            _ => column.collation = 100,
        }
        let joined = super::joins::inner(&left, &changed, "j")?;
        assert!(!joined.proves_key(&["0:id".into()]));
        assert!(joined.proves_key(&["0:id".into(), "1:id".into()]));
    }
    Ok(())
}

#[test]
fn existence_does_not_reuse_navigation_from_before_tagging() -> Result<()> {
    let native = contract();
    let mut compiled = crate::compiler::compile(
        "SELECT a.id FROM public.items a WHERE EXISTS (SELECT 1 FROM public.other b WHERE b.category=a.category)",
        &native,
    )?;
    let position = compiled
        .relational
        .nodes
        .iter()
        .position(|n| matches!(n, Node::LeftJoinInput { spec, .. } if spec.left))
        .ok_or_else(|| anyhow::anyhow!("missing fixture tag"))?;
    let Node::LeftJoinInput { input, .. } = &mut compiled.relational.nodes[position] else {
        anyhow::bail!("missing fixture input");
    };
    let prior = std::mem::replace(input, "prior_key".into());
    compiled.relational.nodes.insert(
        position,
        Node::PartitionBy {
            id: "prior_key".into(),
            input: prior,
            keys: vec![reference("0:id", 20)],
        },
    );
    let id = compiled
        .relational
        .nodes
        .iter()
        .find_map(|n| match n {
            Node::Existence { id, .. } => Some(id.as_str()),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("missing fixture existence"))?;
    let facts = Analysis::infer(&native, compiled.relational())?;
    let filtered = facts.edge(id)?;
    assert!(filtered.proves_key(&["0:id".into()]));
    assert!(filtered.navigation.is_empty());
    let mut probe = Analysis::source_relation(&native.relations[0], 2)?;
    probe.navigation = vec!["2:id".into()];
    let joined = super::joins::inner(&probe, filtered, "downstream")?;
    assert!(!joined.proves_key(&["2:id".into()]));
    assert!(joined.proves_key(&["2:id".into(), "0:id".into()]));
    Ok(())
}

#[test]
fn direct_scalar_identity_retains_modifiers_while_plain_numeric_cast_clears() -> Result<()> {
    for (oid, modifier) in [(1700, (6 << 16 | 2) + 4), (1043, 10)] {
        let mut native = contract();
        let column = &mut native.relations[0].columns[1];
        column.oid = oid;
        column.modifier = modifier;
        column.collation = if oid == 1043 { 100 } else { 0 };
        let mut compiled = crate::compiler::compile("SELECT category FROM public.items", &native)?;
        let input = compiled.output().columns[0].column.clone();
        let mut output = input.clone();
        output.name = "identity_copy".into();
        let ir = &mut compiled.relational;
        let Some(Node::Output { input: edge, .. }) = ir.nodes.last_mut() else {
            anyhow::bail!("missing output")
        };
        let prior = std::mem::replace(edge, "identity".into());
        ir.nodes.insert(
            ir.nodes.len() - 1,
            Node::Map {
                id: "identity".into(),
                input: prior,
                computed: vec![crate::compiler::scalar::Computed {
                    column: output.clone(),
                    expression: crate::compiler::typed::Typed::column(input),
                }],
            },
        );
        ir.output.columns[0].column = output;
        assert_eq!(
            Analysis::infer(&native, ir)?.edge("project")?.columns["identity_copy"].modifier,
            modifier
        );
        let derived = crate::compiler::compile(
            "SELECT q.category FROM(SELECT category FROM public.items)q",
            &native,
        )?;
        let field = &derived.output().columns[0].column.name;
        assert_eq!(
            Analysis::infer(&native, derived.relational())?.edge("project")?.columns[field]
                .modifier,
            modifier
        );
        if oid == 1700 {
            let cast =
                crate::compiler::compile("SELECT category::numeric FROM public.items", &native)?;
            let field = &cast.output().columns[0].column.name;
            assert_eq!(
                Analysis::infer(&native, cast.relational())?.edge("project")?.columns[field]
                    .modifier,
                -1
            );
        }
    }
    Ok(())
}

#[test]
fn source_key_proofs_only_certify_inputs_with_unit_integrated_weights() -> Result<()> {
    let native = contract();
    for (expected, sql) in [
        (true, "SELECT category,COUNT(*) FROM public.items GROUP BY category"),
        (
            true,
            "SELECT d.category,COUNT(*) FROM (SELECT category FROM public.items) d GROUP BY d.category",
        ),
        (
            true,
            "SELECT d.r,COUNT(*) FROM (SELECT category/2 AS r FROM public.items) d GROUP BY d.r",
        ),
        (
            true,
            "WITH c AS (SELECT category FROM public.items) SELECT category,COUNT(*) FROM c GROUP BY category",
        ),
        (true, "SELECT category,COUNT(*) FROM public.items WHERE id>0 GROUP BY category"),
        (
            true,
            "SELECT i.category,COUNT(*) FROM public.items i JOIN public.other o ON i.category=o.category GROUP BY i.category",
        ),
        (
            true,
            "SELECT i.category,COUNT(*) FROM public.items i LEFT JOIN public.other o ON i.category=o.category GROUP BY i.category",
        ),
        (
            true,
            "SELECT u.g,COUNT(*) FROM (SELECT category AS g FROM public.items UNION SELECT category FROM public.other) u GROUP BY u.g",
        ),
        (
            false,
            "SELECT i.category,COUNT(*) FROM public.items i FULL JOIN public.other o ON i.category=o.category GROUP BY i.category",
        ),
        (
            false,
            "SELECT u.g,COUNT(*) FROM (SELECT category AS g FROM public.items UNION ALL SELECT category FROM public.other) u GROUP BY u.g",
        ),
        (
            false,
            "SELECT u.g,COUNT(*) FROM (SELECT category AS g FROM public.items INTERSECT ALL SELECT category FROM public.other) u GROUP BY u.g",
        ),
        (
            false,
            "SELECT u.g,COUNT(*) FROM (SELECT category AS g FROM public.items EXCEPT ALL SELECT category FROM public.other) u GROUP BY u.g",
        ),
    ] {
        let compiled = crate::compiler::compile(sql, &native)?;
        let certified = Analysis::infer(&native, compiled.relational())?
            .committed_statistics(compiled.relational())?;
        assert_eq!(!certified.is_empty(), expected, "{sql}");
    }
    Ok(())
}
