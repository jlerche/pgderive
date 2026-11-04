use super::graph::Graph;
use crate::compiler::{
    ColumnRef,
    projection::Projected,
    relational::{Node, Relational, Source, column_names},
};
use anyhow::{Context, Result};

#[test]
fn shared_ancestry_keeps_input_order_and_visits_each_edge_once() -> Result<()> {
    let contract = crate::source::tests::contract();
    let mut ir = Relational {
        sources: vec![Source { schema: "public".into(), table: "items".into() }],
        projection: Vec::new(),
        output: Projected { columns: Vec::new() },
        nodes: vec![
            Node::Source { id: "source".into(), source: 0 },
            Node::Map { id: "left".into(), input: "source".into(), computed: Vec::new() },
            Node::Map { id: "right".into(), input: "source".into(), computed: Vec::new() },
            Node::Union { id: "union".into(), left: "left".into(), right: "right".into() },
            Node::Output { id: "output".into(), input: "union".into() },
            Node::Source { id: "unused".into(), source: 0 },
        ],
    };
    let mut graph = Graph::new(&mut ir, &contract);
    let order =
        graph.ancestors("output")?.iter().map(|node| node.id().to_owned()).collect::<Vec<_>>();
    assert_eq!(order, ["source", "left", "right", "union", "output"]);
    assert_eq!(graph.sources("output")?, ["source".to_owned()].into());
    assert!(graph.only_consumer("source").is_none());
    assert!(graph.only_consumer("unused").is_none());
    assert_eq!(graph.only_consumer("left").map(Node::id), Some("union"));
    assert!(graph.ancestors("missing").is_err());
    graph.sort()?;
    assert_eq!(graph.ir.nodes.iter().map(Node::id).collect::<Vec<_>>(), order);
    Ok(())
}

#[test]
fn source_aliases_keep_distinct_qualified_columns() -> Result<()> {
    let contract = crate::source::tests::contract();
    let source = Source { schema: "public".into(), table: "items".into() };
    let mut ir = Relational {
        sources: vec![source.clone(), source],
        projection: Vec::new(),
        output: Projected { columns: Vec::new() },
        nodes: vec![
            Node::Source { id: "left".into(), source: 0 },
            Node::Source { id: "right".into(), source: 1 },
        ],
    };
    let graph = Graph::new(&mut ir, &contract);
    assert_eq!(graph.columns("left")?, ["0:id".to_owned()].into());
    assert_eq!(graph.columns("right")?, ["1:id".to_owned()].into());
    Ok(())
}

#[test]
fn column_discovery_preserves_qualified_names() -> Result<()> {
    let columns = ["0:id", "1:id", "@scope:0:id"].map(|name| ColumnRef {
        right: false,
        name: name.to_owned(),
        oid: 23,
        nullable: false,
    });
    assert_eq!(column_names(&columns)?, columns.iter().map(|column| column.name.clone()).collect());
    Ok(())
}

mod scope;

fn plan(nodes: Vec<Node>) -> Relational {
    let source = Source { schema: "public".into(), table: "items".into() };
    Relational {
        sources: vec![source.clone(), source],
        projection: Vec::new(),
        output: Projected { columns: Vec::new() },
        nodes,
    }
}

#[test]
fn logical_join_view_unwraps_presence_and_keeps_attachment_identity() -> Result<()> {
    use super::join::Join;
    use crate::compiler::left_join::Input;
    let contract = crate::source::tests::contract();
    let column =
        |name: &str| ColumnRef { name: name.into(), right: false, oid: 20, nullable: false };
    let mut ir = plan(vec![
        Node::Source { id: "left".into(), source: 0 },
        Node::Source { id: "right".into(), source: 1 },
        Node::LeftJoinInput {
            id: "left_tag".into(),
            input: "left".into(),
            spec: Input { left: true, keys: vec![column("0:id")] },
        },
        Node::LeftJoinInput {
            id: "right_tag".into(),
            input: "right".into(),
            spec: Input { left: false, keys: vec![column("1:id")] },
        },
        Node::RightCounts { id: "counts".into(), input: "right_tag".into() },
        Node::RightPresence { id: "presence".into(), input: "counts".into() },
        Node::Union { id: "union".into(), left: "left_tag".into(), right: "presence".into() },
        Node::Existence { id: "join".into(), input: "union".into(), anti: true },
    ]);
    let graph = Graph::new(&mut ir, &contract);
    let (join, left) = Join::above(&graph, "right_tag")?.context("missing join owner")?;
    assert!(!left);
    assert!(matches!(join.node, Node::Existence { anti: true, .. }));
    assert_eq!((join.right.id.as_str(), join.right.input.as_str()), ("right_tag", "right"));
    assert_eq!(join.right.keys, vec![column("1:id")]);
    assert!(Join::above(&graph, "left_tag")?.context("missing left owner")?.1);
    graph.ir.nodes.push(Node::Output { id: "fork".into(), input: "right_tag".into() });
    assert!(Join::above(&graph, "right_tag")?.is_none());
    Ok(())
}

#[test]
fn stale_filter_attachment_fails_without_creating_an_orphan() -> Result<()> {
    use crate::compiler::expression::Expr;
    let contract = crate::source::tests::contract();
    let mut ir = plan(vec![
        Node::Source { id: "source".into(), source: 0 },
        Node::CrossKey { id: "key".into(), input: "source".into() },
    ]);
    let mut graph = Graph::new(&mut ir, &contract);
    graph.filter_edge("key", "source", vec![Expr::And(Vec::new())])?;
    let before = graph.ir.nodes.len();
    assert!(graph.filter_edge("key", "source", vec![Expr::And(Vec::new())]).is_err());
    assert_eq!(graph.ir.nodes.len(), before);
    Ok(())
}
