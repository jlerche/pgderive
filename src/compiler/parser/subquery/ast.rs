//! Small native AST constructors shared by subquery decorrelation.
use pg_query::{Node, NodeEnum, protobuf as pg};
pub(super) const VALUE: &str = "\0value";
pub(super) const PRESENT: &str = "\0present";
pub(super) const ROWS: &str = "\0rows";
pub(super) fn column(alias: &str, name: &str) -> Node {
    Node {
        node: Some(NodeEnum::ColumnRef(pg::ColumnRef {
            fields: [alias, name].into_iter().map(string).collect(),
            location: -1,
        })),
    }
}
pub(super) fn string(value: &str) -> Node {
    Node { node: Some(NodeEnum::String(pg::String { sval: value.into() })) }
}
pub(super) fn target(value: Node, label: &str) -> Node {
    Node {
        node: Some(NodeEnum::ResTarget(Box::new(pg::ResTarget {
            name: label.into(),
            val: Some(Box::new(value)),
            ..pg::ResTarget::default()
        }))),
    }
}
pub(super) fn boolean(value: bool) -> Node {
    Node {
        node: Some(NodeEnum::AConst(pg::AConst {
            val: Some(pg::a_const::Val::Boolval(pg::Boolean { boolval: value })),
            ..pg::AConst::default()
        })),
    }
}
pub(super) fn null() -> Node {
    Node { node: Some(NodeEnum::AConst(pg::AConst { isnull: true, ..pg::AConst::default() })) }
}
pub(super) fn comparison(left: Node, right: Node, name: &str) -> Node {
    Node {
        node: Some(NodeEnum::AExpr(Box::new(pg::AExpr {
            kind: i32::from(pg::AExprKind::AexprOp),
            name: vec![string(name)],
            lexpr: Some(Box::new(left)),
            rexpr: Some(Box::new(right)),
            location: -1,
        }))),
    }
}
pub(super) fn logical(args: Vec<Node>, and: bool) -> Option<Box<Node>> {
    match args.len() {
        0 => None,
        1 => args.into_iter().next().map(Box::new),
        _ => Some(Box::new(boolean_expression(
            args,
            if and { pg::BoolExprType::AndExpr } else { pg::BoolExprType::OrExpr },
        ))),
    }
}
pub(super) fn boolean_expression(args: Vec<Node>, kind: pg::BoolExprType) -> Node {
    Node {
        node: Some(NodeEnum::BoolExpr(Box::new(pg::BoolExpr {
            boolop: i32::from(kind),
            args,
            ..pg::BoolExpr::default()
        }))),
    }
}
pub(super) fn is_null(value: Node) -> Node {
    Node {
        node: Some(NodeEnum::NullTest(Box::new(pg::NullTest {
            arg: Some(Box::new(value)),
            nulltesttype: i32::from(pg::NullTestType::IsNull),
            ..pg::NullTest::default()
        }))),
    }
}
pub(super) fn case(condition: Node, result: Node, otherwise: Node) -> Node {
    Node {
        node: Some(NodeEnum::CaseExpr(Box::new(pg::CaseExpr {
            args: vec![Node {
                node: Some(NodeEnum::CaseWhen(Box::new(pg::CaseWhen {
                    expr: Some(Box::new(condition)),
                    result: Some(Box::new(result)),
                    ..pg::CaseWhen::default()
                }))),
            }],
            defresult: Some(Box::new(otherwise)),
            ..pg::CaseExpr::default()
        }))),
    }
}
pub(super) fn count() -> Node {
    Node {
        node: Some(NodeEnum::FuncCall(Box::new(pg::FuncCall {
            funcname: vec![string("count")],
            agg_star: true,
            funcformat: i32::from(pg::CoercionForm::CoerceExplicitCall),
            ..pg::FuncCall::default()
        }))),
    }
}

pub(super) fn checked_scalar(value: Node, multiple: Node) -> Node {
    Node {
        node: Some(NodeEnum::FuncCall(Box::new(pg::FuncCall {
            funcname: vec![string(crate::compiler::subquery::CHECK)],
            args: vec![value, multiple],
            funcformat: i32::from(pg::CoercionForm::CoerceExplicitCall),
            ..pg::FuncCall::default()
        }))),
    }
}

pub(super) fn not(value: Node) -> Node {
    Node {
        node: Some(NodeEnum::BoolExpr(Box::new(pg::BoolExpr {
            boolop: i32::from(pg::BoolExprType::NotExpr),
            args: vec![value],
            ..pg::BoolExpr::default()
        }))),
    }
}
