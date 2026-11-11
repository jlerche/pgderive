//! Normalize top-level WHERE, HAVING and ON conjuncts before folding them.
use super::Expr;

impl Expr {
    pub(in crate::compiler) fn into_quals(self) -> Self {
        let mut quals = Vec::new();
        self.collect_quals(&mut quals);
        if quals.len() == 1 {
            return quals.remove(0);
        }
        Self::Quals(quals)
    }
    /// `simplify_and_arguments` stops at a constant false, before sorting can reorder a clause.
    pub(in crate::compiler) fn until_false(self) -> Self {
        let Self::And(values) = self else { return self };
        let mut written = Vec::new();
        for value in values.into_iter().map(Self::until_false) {
            let conjuncts = if let Self::And(inner) = value { inner } else { vec![value] };
            for value in conjuncts {
                let decided = value.constant_false();
                written.push(value);
                if decided {
                    return Self::And(written);
                }
            }
        }
        Self::And(written)
    }
    fn constant_false(&self) -> bool {
        self.fold().is_ok_and(|value| {
            value.is_constant() && matches!(value.constant_truth(), Ok(Some(false)))
        })
    }
    fn collect_quals(self, quals: &mut Vec<Self>) {
        match self {
            Self::And(values) => values.into_iter().for_each(|value| value.collect_quals(quals)),
            Self::Not(value) => match *value {
                Self::Or(values) => {
                    for value in values {
                        value.negated().collect_quals(quals);
                    }
                }
                Self::Not(value) => value.collect_quals(quals),
                value => quals.push(Self::Not(Box::new(value))),
            },
            Self::Or(values) => match factored(values) {
                Self::Or(values) => quals.push(Self::Or(values)),
                value => value.collect_quals(quals),
            },
            value => quals.push(value),
        }
    }
    pub(super) const fn is_equality(&self) -> bool {
        matches!(self, Self::Compare(super::Compare::Eq, _, _))
    }
}
fn clauses(arm: &Expr) -> &[Expr] {
    match arm {
        Expr::And(values) => values,
        value => std::slice::from_ref(value),
    }
}
fn factored(arms: Vec<Expr>) -> Expr {
    let shortest = arms.iter().map(clauses).min_by_key(|values| values.len()).unwrap_or(&[]);
    let common = shortest
        .iter()
        .filter(|value| arms.iter().all(|arm| clauses(arm).contains(value)))
        .cloned()
        .collect::<Vec<_>>();
    if common.is_empty() {
        return Expr::Or(arms);
    }
    let remainders = arms
        .iter()
        .map(|arm| {
            clauses(arm).iter().filter(|value| !common.contains(value)).cloned().collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut factors = common;
    if remainders.iter().all(|values| !values.is_empty()) {
        factors.push(Expr::Or(remainders.into_iter().map(conjunction).collect()));
    }
    conjunction(factors)
}
fn conjunction(mut values: Vec<Expr>) -> Expr {
    if values.len() == 1 {
        return values.remove(0);
    }
    Expr::And(values)
}
