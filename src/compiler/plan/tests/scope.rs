use super::{Graph, plan};
use crate::compiler::{
    ColumnRef, Predicate,
    expression::Expr,
    plan::scope::Scopes,
    relational::{Binding, Node},
    scalar::Computed,
    setop::{Kind, Spec},
    typed::Typed,
};
use anyhow::Result;

fn column(name: &str, oid: u32) -> ColumnRef {
    ColumnRef { right: false, name: name.into(), oid, nullable: false, modifier: None }
}
fn source() -> Node {
    Node::Source { id: "source".into(), source: 0 }
}
fn alignment(input: &str) -> Result<Vec<Node>> {
    let cast = column("@union_cast_0", 1700);
    Ok(vec![
        Node::Map {
            id: "cast".into(),
            input: input.into(),
            computed: vec![Computed {
                column: cast.clone(),
                expression: Typed::set_cast(column("0:id", 20), 1700, None)?,
            }],
        },
        Node::Rebind {
            id: "arm".into(),
            input: "cast".into(),
            fields: vec![Binding { input: cast, output: column("@union_0", 1700) }],
        },
    ])
}

#[test]
fn materialization_blocks_flattening_but_allows_a_simple_append_member() -> Result<()> {
    let contract = crate::source::tests::contract();
    let mut ir = plan(vec![
        source(),
        Node::Rebind {
            id: "opaque_materialized".into(),
            input: "source".into(),
            fields: vec![Binding { input: column("0:id", 20), output: column("renamed", 20) }],
        },
    ]);
    let graph = Graph::new(&mut ir, &contract);
    let scopes = Scopes::new(&graph, ["opaque_materialized"])?;
    let scope = scopes.at("opaque_materialized")?;
    assert!(scope.simple);
    assert!(!scope.single);
    assert!(!scope.flat);
    Ok(())
}

#[test]
fn common_type_alignment_keeps_an_append_arm_unflattened() -> Result<()> {
    let contract = crate::source::tests::contract();
    let mut nodes = vec![source()];
    nodes.extend(alignment("source")?);
    let mut ir = plan(nodes);
    let graph = Graph::new(&mut ir, &contract);
    let scopes = Scopes::new(&graph, ["arm"])?;
    let scope = scopes.at("arm")?;
    assert!(scope.arm);
    assert!(scope.single);
    assert!(!scope.simple);
    assert!(!scope.flat);
    assert_eq!(scope.coerced, ["@union_0".to_owned()].into());
    Ok(())
}

#[test]
fn filter_is_an_alignment_and_except_boundary_but_retains_arm_identity() -> Result<()> {
    let contract = crate::source::tests::contract();
    let mut nodes = vec![
        source(),
        Node::SetOperation {
            id: "difference".into(),
            input: "source".into(),
            spec: Spec { kind: Kind::Except, all: true, columns: vec![column("0:id", 20)] },
        },
    ];
    nodes.extend(alignment("difference")?);
    nodes.push(Node::Filter {
        id: "filtered".into(),
        input: "arm".into(),
        predicate: Predicate::new(Expr::And(Vec::new())),
    });
    let mut ir = plan(nodes);
    let graph = Graph::new(&mut ir, &contract);
    let scopes = Scopes::new(&graph, ["filtered"])?;
    let below = scopes.at("arm")?;
    assert!(below.except);
    assert!(!below.coerced.is_empty());
    let filtered = scopes.at("filtered")?;
    assert!(filtered.arm);
    assert!(!filtered.except);
    assert!(filtered.coerced.is_empty());
    assert!(!filtered.single);
    assert_eq!(filtered.simple, below.simple);
    assert_eq!(filtered.flat, below.flat);
    Ok(())
}

#[test]
fn requested_roots_support_shared_out_of_order_inputs_and_ignore_unrelated_edges() -> Result<()> {
    let contract = crate::source::tests::contract();
    let mut ir = plan(vec![
        Node::Union { id: "union".into(), left: "left".into(), right: "right".into() },
        Node::Map { id: "left".into(), input: "source".into(), computed: Vec::new() },
        Node::Map { id: "right".into(), input: "source".into(), computed: Vec::new() },
        source(),
        Node::Empty { id: "unrelated".into(), input: "missing".into() },
    ]);
    let graph = Graph::new(&mut ir, &contract);
    let scopes = Scopes::new(&graph, ["union"])?;
    assert!(scopes.at("source")?.single);
    assert!(scopes.at("union")?.flat);
    assert!(scopes.at("unrelated").is_err());
    assert!(Scopes::new(&graph, ["unrelated"]).is_err());
    Ok(())
}
