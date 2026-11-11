//! `PostgreSQL` per-tuple qual cost, in half operators, for ordering top-level quals.
use super::{Arithmetic, Typed, Value};

pub const OPERATOR_COST: u64 = 2;

impl Typed {
    pub(in crate::compiler) fn cost(&self) -> u64 {
        if self.is_constant() {
            return 0;
        }
        match &self.value {
            Value::Column(_)
            | Value::Average(_)
            | Value::Literal(_)
            | Value::Numeric(_)
            | Value::Text(_)
            | Value::Encoded(_)
            | Value::Interval(_) => 0,
            Value::Boolean(value) => value.cost(),
            Value::Temporal(_) => OPERATOR_COST,
            Value::Call(call) => call.arguments.iter().fold(OPERATOR_COST, |total, argument| {
                total
                    .saturating_add(argument.cost())
                    .saturating_add(promotion(argument, call.input_oid))
            }),
            Value::Cast { input, typmod } => {
                input.cost().saturating_add(cast_cost(input.oid, self.oid, typmod.is_some()))
            }
            Value::Binary { operation, left, right }
                if self.oid != 1700
                    && (operation.is_bitwise() || *operation == Arithmetic::Remainder) =>
            {
                let right_oid = if operation.is_shift() { 23 } else { self.oid };
                Self::operator_cost(left, right)
                    .saturating_add(implicit_cast(left, self.oid))
                    .saturating_add(implicit_cast(right, right_oid))
            }
            Value::Binary { left, right, .. }
            | Value::NullIf { left, right }
            | Value::Concat { left, right } => Self::operator_cost(left, right),
            Value::Coalesce(values) => values.iter().map(Self::cost).sum(),
            Value::MinMax { arguments, .. } => {
                arguments.iter().map(Self::cost).sum::<u64>().saturating_add(OPERATOR_COST)
            }
            Value::SimpleCase(case) => case.cost(),
            Value::Case { branches, otherwise } => branches
                .iter()
                .map(|(condition, value)| condition.cost().saturating_add(value.cost()))
                .sum::<u64>()
                .saturating_add(otherwise.cost()),
        }
    }
    pub(in crate::compiler) fn operator_cost(left: &Self, right: &Self) -> u64 {
        Self::comparison_cost(left, right).saturating_add(left.cost())
    }

    pub(super) fn comparison_cost(left: &Self, right: &Self) -> u64 {
        OPERATOR_COST
            .saturating_add(right.cost())
            .saturating_add(promotion(left, right.oid))
            .saturating_add(promotion(right, left.oid))
    }
}
fn promotion(value: &Typed, target: u32) -> u64 {
    if target == 1700 && matches!(value.oid, 20 | 21 | 23) && !value.is_constant() {
        OPERATOR_COST
    } else {
        0
    }
}
fn implicit_cast(value: &Typed, oid: u32) -> u64 {
    if value.is_constant() { 0 } else { cast_cost(value.oid, oid, false) }
}
fn cast_cost(input: u32, output: u32, typmod: bool) -> u64 {
    let text = |oid| matches!(oid, 25 | 1043);
    if text(input) && text(output) && !typmod {
        0
    } else if input == output {
        if typmod { OPERATOR_COST } else { 0 }
    } else if (input, output) == (16, 25) {
        OPERATOR_COST
    } else if text(input) || text(output) || typmod {
        2 * OPERATOR_COST
    } else {
        OPERATOR_COST
    }
}
