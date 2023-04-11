// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;

use mz_repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};

use crate::{
    BinaryFunc, EvalError, MapFilterProject, MfpPlan, MirScalarExpr, UnaryFunc,
    UnmaterializableFunc, VariadicFunc,
};

/// Tracks whether a function is monotonic, either increasing or decreasing.
/// This property is useful for static analysis because a monotonic function maps ranges to ranges:
/// ie. if `a` is between `b` and `c` inclusive, then `f(a)` is between `f(b)` and `f(c)` inclusive.
/// (However, if either `f(b)` and `f(c)` error or return null, we don't assume anything about `f(a)`.
///
/// This should likely be moved to a function annotation in the future.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Monotonic {
    /// Monotonic in this argument.
    Yes,
    /// We haven't classified this one yet. (Conservatively treated as "no", but a distinct
    /// enum so it's easy to tell whether someone's thought about a particular function yet.)
    Maybe,
    /// We don't know any useful properties of this function.
    No,
}

fn unary_monotonic(func: &UnaryFunc) -> Monotonic {
    use Monotonic::*;
    use UnaryFunc::*;
    match func {
        // Casts are generally monotonic.
        CastUint64ToNumeric(_)
        | CastNumericToUint64(_)
        | CastNumericToMzTimestamp(_)
        | CastUint64ToMzTimestamp(_)
        | CastTimestampToMzTimestamp(_)
        | CastTimestampTzToMzTimestamp(_) => Yes,
        // Negation is monotonically decreasing, but that's fine.
        NegInt64(_) | NegNumeric(_) | Not(_) => Yes,
        // Trivially monotonic, since all non-none values map to `false`.
        IsNull(_) => Yes,
        AbsInt64(_) => No,
        _ => Maybe,
    }
}

fn binary_monotonic(func: &BinaryFunc) -> (Monotonic, Monotonic) {
    use BinaryFunc::*;
    use Monotonic::*;

    match func {
        AddInt64 | MulInt64 | AddNumeric | MulNumeric | SubInt64 | SubNumeric => (Yes, Yes),
        // Monotonic in the left argument, but the right hand side has a discontinuity.
        // Could be treated as monotonic if we also captured the valid domain of the function args.
        DivInt64 | DivNumeric => (Yes, No),
        Lt | Lte | Gt | Gte => (Yes, Yes),
        _ => (Maybe, Maybe),
    }
}

fn variadic_monotonic(func: &VariadicFunc) -> Monotonic {
    use Monotonic::*;
    use VariadicFunc::*;
    match func {
        And | Or => Yes,
        _ => Maybe,
    }
}

/// An inclusive range of non-null datum values.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Values<'a> {
    /// This range contains no values.
    Empty,
    /// An inclusive range. Invariant: the first element is always <= the second.
    Within(Datum<'a>, Datum<'a>),
    /// This range might contain any value. Since we're overapproximating, this is often used
    /// as a "safe fallback" when we can't determine the right boundaries for a range.
    All,
}

impl<'a> Default for Values<'a> {
    fn default() -> Self {
        Values::Empty
    }
}

impl<'a> Values<'a> {
    fn just(a: Datum<'a>) -> Values<'a> {
        Self::Within(a, a)
    }

    fn within(a: Datum<'a>, b: Datum<'a>) -> Values<'a> {
        if a <= b {
            Self::Within(a, b)
        } else {
            Self::Within(b, a)
        }
    }

    fn union(self, other: Values<'a>) -> Values<'a> {
        match (self, other) {
            (Values::Empty, r) => r,
            (r, Values::Empty) => r,
            (Values::Within(a0, a1), Values::Within(b0, b1)) => {
                Values::within(a0.min(b0), a1.max(b1))
            }
            (Values::All, _) => Values::All,
            (_, Values::All) => Values::All,
        }
    }

    fn intersect(self, other: Values<'a>) -> Values<'a> {
        match (self, other) {
            (Values::Empty, _) => Values::Empty,
            (_, Values::Empty) => Values::Empty,
            (Values::Within(a0, a1), Values::Within(b0, b1)) => {
                let min = a0.max(b0);
                let max = a1.min(b1);
                if min <= max {
                    Values::within(min, max)
                } else {
                    Values::Empty
                }
            }
            (Values::All, v) => v,
            (v, Values::All) => v,
        }
    }
}

/// An approximation of the set of values an expression might have, including whether or not it
/// might be null or an error value. This is generally an _overapproximation_, in the sense that
/// [ResultSpec::may_contain] may return true even if the argument is not included in the set.
/// (However, it should never return false when the value _is_ included!)
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ResultSpec<'a> {
    nullable: bool,
    fallible: bool,
    values: Values<'a>,
}

impl<'a> ResultSpec<'a> {
    pub fn nothing() -> Self {
        ResultSpec {
            nullable: false,
            fallible: false,
            values: Values::Empty,
        }
    }

    pub fn anything() -> Self {
        ResultSpec {
            nullable: true,
            fallible: true,
            values: Values::All,
        }
    }

    pub fn null() -> Self {
        ResultSpec {
            nullable: true,
            ..Self::nothing()
        }
    }

    pub fn fails() -> Self {
        ResultSpec {
            fallible: true,
            ..Self::nothing()
        }
    }

    pub fn has_type(col: &ColumnType, fallible: bool) -> ResultSpec<'a> {
        let values = match &col.scalar_type {
            ScalarType::Bool => Values::within(Datum::False, Datum::True),
            _ => Values::All,
        };
        ResultSpec {
            nullable: col.nullable,
            fallible,
            values,
        }
    }

    pub fn value_between(min: Datum<'a>, max: Datum<'a>) -> ResultSpec<'a> {
        ResultSpec {
            values: Values::Within(min, max),
            ..ResultSpec::nothing()
        }
    }

    pub fn union(self, other: ResultSpec<'a>) -> ResultSpec<'a> {
        ResultSpec {
            nullable: self.nullable || other.nullable,
            fallible: self.fallible || other.fallible,
            values: self.values.union(other.values),
        }
    }

    pub fn intersect(self, other: ResultSpec<'a>) -> ResultSpec<'a> {
        ResultSpec {
            nullable: self.nullable && other.nullable,
            fallible: self.fallible && other.fallible,
            values: self.values.intersect(other.values),
        }
    }

    pub fn may_contain(&self, value: Datum<'a>) -> bool {
        if value == Datum::Null {
            return self.nullable;
        }
        match &self.values {
            Values::Empty => false,
            Values::Within(min, max) => *min <= value && value <= *max,
            Values::All => true,
        }
    }

    pub fn may_fail(&self) -> bool {
        self.fallible
    }

    /// This function "maps" a function across the set of valid datums, returning the set of possible
    /// results.
    ///
    /// If we actually stored a set of every possible value, this could by done by passing
    /// each datup to the function one-by-one and unioning the resulting sets. This is possible
    /// when our values set is empty or contains a single datum, but when it contains a range,
    /// we need a little metadata about the function to be able to infer a useful result.
    fn flat_map(
        self,
        is_monotonic: Monotonic,
        mut datum_map: impl FnMut(Datum<'a>) -> ResultSpec<'a>,
    ) -> ResultSpec<'a> {
        let null_spec = if self.nullable {
            datum_map(Datum::Null)
        } else {
            ResultSpec::nothing()
        };

        let error_spec = ResultSpec {
            nullable: false,
            fallible: self.fallible,
            values: Values::Empty,
        };

        let values_spec = match self.values {
            Values::Empty => ResultSpec::nothing(),
            // If this range contains a single datum, just call the function.
            Values::Within(min, max) if min == max => datum_map(min),
            // If this is a range of booleans, we know all the values... just try them.
            Values::Within(Datum::False, Datum::True) => {
                datum_map(Datum::False).union(datum_map(Datum::True))
            }
            // Otherwise, if our function is monotonic, we can try mapping the input
            // range to an output range.
            Values::Within(min, max) => match is_monotonic {
                Monotonic::Yes => {
                    let min_column = datum_map(min);
                    let max_column = datum_map(max);
                    if min_column.nullable
                        || min_column.fallible
                        || max_column.nullable
                        || max_column.fallible
                    {
                        ResultSpec::anything()
                    } else {
                        min_column.union(max_column)
                    }
                }
                Monotonic::Maybe | Monotonic::No => ResultSpec::anything(),
            },
            Values::All => ResultSpec::anything(),
        };

        null_spec.union(error_spec).union(values_spec)
    }
}

/// [Abstract interpretation](https://en.wikipedia.org/wiki/Abstract_interpretation) for
/// [MirScalarExpr].
///
/// [MirScalarExpr::eval] implements a "concrete interpreter" for the expression type: given an
/// expression and specific column values as input, it returns a specific value for the output.
/// This could be reimplemented using this trait... but it's most useful for "abstract"
/// interpretations of the expression, where we generalize about sets of possible inputs and outputs.
/// See [Trace] and [ColumnSpecs] for how this can be useful in practice.
pub trait Interpreter {
    type Summary: Sized + Clone;

    /// A column of the input row.
    fn eval_column(&self, id: usize) -> Self::Summary;

    /// A literal value.
    /// (Stored as a row, because we can't own a Datum.)
    fn eval_literal(&self, result: &Result<Row, EvalError>, col_type: &ColumnType)
        -> Self::Summary;

    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `MirScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    fn eval_unmaterializable(&self, func: &UnmaterializableFunc) -> Self::Summary;

    /// A function call that takes one expression as an argument.
    fn eval_unary(&self, func: &UnaryFunc, expr: Self::Summary) -> Self::Summary;

    /// A function call that takes two expressions as arguments.
    fn eval_binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary;

    /// A function call that takes an arbitrary number of arguments.
    fn eval_variadic(&self, func: &VariadicFunc, exprs: Vec<Self::Summary>) -> Self::Summary;

    /// Conditionally evaluated expressions.
    fn eval_if(
        &self,
        cond: Self::Summary,
        then: Self::Summary,
        els: Self::Summary,
    ) -> Self::Summary;

    fn eval_expr(&self, expr: &MirScalarExpr) -> Self::Summary {
        match expr {
            MirScalarExpr::Column(id) => self.eval_column(*id),
            MirScalarExpr::Literal(value, col_type) => self.eval_literal(value, col_type),
            MirScalarExpr::CallUnmaterializable(func) => self.eval_unmaterializable(func),
            MirScalarExpr::CallUnary { func, expr } => {
                let expr_range = self.eval_expr(expr);
                self.eval_unary(func, expr_range)
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                let expr1_range = self.eval_expr(expr1);
                let expr2_range = self.eval_expr(expr2);
                self.eval_binary(func, expr1_range, expr2_range)
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                let exprs: Vec<_> = exprs.into_iter().map(|e| self.eval_expr(e)).collect();
                self.eval_variadic(func, exprs)
            }
            MirScalarExpr::If { cond, then, els } => {
                let cond_range = self.eval_expr(cond);
                let then_range = self.eval_expr(then);
                let els_range = self.eval_expr(els);
                self.eval_if(cond_range, then_range, els_range)
            }
        }
    }

    fn eval_mfp(&self, mfp: &MapFilterProject) -> Self::Summary {
        let mfp_eval = MfpEval::new(self, mfp.input_arity, &mfp.expressions);
        // NB: self should not be used after this point!
        let predicates = mfp
            .predicates
            .iter()
            .map(|(_, e)| mfp_eval.eval_expr(e))
            .collect();
        mfp_eval.eval_variadic(&VariadicFunc::And, predicates)
    }

    fn eval_mfp_plan(&self, plan: &MfpPlan) -> Self::Summary {
        let mfp_eval = MfpEval::new(self, plan.mfp.input_arity, &plan.mfp.expressions);
        // NB: self should not be used after this point!
        let mut results: Vec<_> = plan
            .mfp
            .predicates
            .iter()
            .map(|(_, e)| mfp_eval.eval_expr(e))
            .collect();
        let mz_now = mfp_eval.eval_unmaterializable(&UnmaterializableFunc::MzNow);
        for bound in &plan.lower_bounds {
            let bound_range = mfp_eval.eval_expr(bound);
            let result = mfp_eval.eval_binary(&BinaryFunc::Lte, bound_range, mz_now.clone());
            results.push(result);
        }
        for bound in &plan.upper_bounds {
            let bound_range = mfp_eval.eval_expr(bound);
            let result = mfp_eval.eval_binary(&BinaryFunc::Gte, bound_range, mz_now.clone());
            results.push(result);
        }
        self.eval_variadic(&VariadicFunc::And, results)
    }
}

/// Wrap another interpreter, but tack a few extra columns on at the end. An internal implementation
/// detail of `eval_mfp` and `eval_mfp_plan`.
struct MfpEval<'a, E: Interpreter + ?Sized> {
    evaluator: &'a E,
    input_arity: usize,
    expressions: Vec<E::Summary>,
}

impl<'a, E: Interpreter + ?Sized> MfpEval<'a, E> {
    fn new(evaluator: &'a E, input_arity: usize, expressions: &[MirScalarExpr]) -> Self {
        let mut mfp_eval = MfpEval {
            evaluator,
            input_arity,
            expressions: vec![],
        };
        for expr in expressions {
            let result = mfp_eval.eval_expr(expr);
            mfp_eval.expressions.push(result);
        }
        mfp_eval
    }
}

impl<'a, E: Interpreter + ?Sized> Interpreter for MfpEval<'a, E> {
    type Summary = E::Summary;

    fn eval_column(&self, id: usize) -> Self::Summary {
        if id < self.input_arity {
            self.evaluator.eval_column(id)
        } else {
            self.expressions[id - self.input_arity].clone()
        }
    }

    fn eval_literal(
        &self,
        result: &Result<Row, EvalError>,
        col_type: &ColumnType,
    ) -> Self::Summary {
        self.evaluator.eval_literal(result, col_type)
    }

    fn eval_unmaterializable(&self, func: &UnmaterializableFunc) -> Self::Summary {
        self.evaluator.eval_unmaterializable(func)
    }

    fn eval_unary(&self, func: &UnaryFunc, expr: Self::Summary) -> Self::Summary {
        self.evaluator.eval_unary(func, expr)
    }

    fn eval_binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary {
        self.evaluator.eval_binary(func, left, right)
    }

    fn eval_variadic(&self, func: &VariadicFunc, exprs: Vec<Self::Summary>) -> Self::Summary {
        self.evaluator.eval_variadic(func, exprs)
    }

    fn eval_if(
        &self,
        cond: Self::Summary,
        then: Self::Summary,
        els: Self::Summary,
    ) -> Self::Summary {
        self.evaluator.eval_if(cond, then, els)
    }
}

/// For a given column and expression, does the range of the column influence the range of the
/// expression? (ie. if the expression is a filter, could we tell that the filter
/// will never return True?)
#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub enum Pushdownable {
    No,
    Maybe,
    Yes,
}

impl Pushdownable {
    fn apply_fn(mut columns: Vec<Self>, is_monotonic: Monotonic) -> Vec<Self> {
        match is_monotonic {
            Monotonic::Yes => {
                // Still correlated, nothing to do.
            }
            Monotonic::No => {
                // A function that we know we can't see through. No reason to expect the range
                // of the columns to affect the range of the expression.
                for col in &mut columns {
                    *col = Pushdownable::No;
                }
            }
            Monotonic::Maybe => {
                // A function that we may or may not be able to see through.
                for col in &mut columns {
                    *col = Pushdownable::Maybe.min(*col);
                }
            }
        }
        columns
    }

    fn merge(mut columns: Vec<Self>, other: Vec<Self>) -> Vec<Self> {
        let new_len = columns.len().max(other.len());
        columns.resize(new_len, Pushdownable::No);
        for (my_col, other_col) in columns.iter_mut().zip(other.iter()) {
            *my_col = (*other_col).max(*my_col);
        }
        columns
    }
}

/// An interpreter that traces how information about the source columns flows through the expression.
///
#[derive(Debug)]
pub struct Trace;

impl Interpreter for Trace {
    /// For every column in the data, we track whether or not that column is "pusdownable".
    /// (If the column is not present in the summary, we default to `false`.
    type Summary = Vec<Pushdownable>;

    fn eval_column(&self, id: usize) -> Self::Summary {
        let mut columns = vec![Pushdownable::No; id];
        columns.push(Pushdownable::Yes);
        columns
    }

    fn eval_literal(
        &self,
        _result: &Result<Row, EvalError>,
        _col_type: &ColumnType,
    ) -> Self::Summary {
        Vec::default()
    }

    fn eval_unmaterializable(&self, _func: &UnmaterializableFunc) -> Self::Summary {
        Vec::default()
    }

    fn eval_unary(&self, func: &UnaryFunc, expr: Self::Summary) -> Self::Summary {
        Pushdownable::apply_fn(expr, unary_monotonic(func))
    }

    fn eval_binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary {
        let (left_monotonic, right_monotonic) = binary_monotonic(func);
        Pushdownable::merge(
            Pushdownable::apply_fn(left, left_monotonic),
            Pushdownable::apply_fn(right, right_monotonic),
        )
    }

    fn eval_variadic(&self, func: &VariadicFunc, exprs: Vec<Self::Summary>) -> Self::Summary {
        let is_monotonic = variadic_monotonic(func);
        exprs.into_iter().fold(Vec::default(), |acc, columns| {
            Pushdownable::merge(acc, Pushdownable::apply_fn(columns, is_monotonic))
        })
    }

    fn eval_if(
        &self,
        cond: Self::Summary,
        then: Self::Summary,
        els: Self::Summary,
    ) -> Self::Summary {
        Pushdownable::merge(cond, Pushdownable::merge(then, els))
    }
}

/// An interpreter that:
/// - stores both the type and the range of possible values for every column and
///   unmaterializable function. (See the `push_` methods.)
/// - given an expression (or MFP, etc.), returns the range of possible results that evaluating that
///   expression might have. (See the `eval_` methods.)
#[derive(Clone, Debug)]
pub struct ColumnSpec<'a> {
    pub col_type: ColumnType,
    pub range: ResultSpec<'a>,
}

#[derive(Clone, Debug)]
pub struct ColumnSpecs<'a> {
    pub columns: Vec<ColumnSpec<'a>>,
    pub unmaterializables: BTreeMap<UnmaterializableFunc, ResultSpec<'a>>,
    pub arena: &'a RowArena,
}

impl<'a> ColumnSpecs<'a> {
    /// Create a new, empty set of column specs. (Initially, the only assumption we make about the
    /// data in the column is that it matches the type.)
    pub fn new(relation: RelationType, arena: &'a RowArena) -> Self {
        ColumnSpecs {
            columns: relation
                .column_types
                .into_iter()
                .map(|ct| ColumnSpec {
                    range: ResultSpec::has_type(&ct, false),
                    col_type: ct,
                })
                .collect(),
            unmaterializables: Default::default(),
            arena,
        }
    }

    /// Restrict the set of possible values in a given column. (By intersecting it with the existing
    /// spec.)
    pub fn push_column(&mut self, id: usize, update: ResultSpec<'a>) {
        let range = &mut self.columns.get_mut(id).expect("valid column id").range;
        *range = range.intersect(update);
    }

    /// Restrict the set of possible values a given unmaterializable func might return. (By
    /// intersecting it with the existing spec.)
    pub fn push_unmaterializable(&mut self, func: UnmaterializableFunc, update: ResultSpec<'a>) {
        let range = self
            .unmaterializables
            .entry(func.clone())
            .or_insert_with(|| ResultSpec::has_type(&func.output_type(), false));
        *range = range.intersect(update);
    }

    fn eval_result<'b, E>(&self, result: Result<Datum<'b>, E>) -> ResultSpec<'a> {
        match result {
            Ok(Datum::Null) => ResultSpec {
                nullable: true,
                ..ResultSpec::nothing()
            },
            Ok(d) => ResultSpec {
                values: Values::just(self.arena.make_datum(|packer| packer.push(d))),
                ..ResultSpec::nothing()
            },
            Err(_) => ResultSpec {
                fallible: true,
                ..ResultSpec::nothing()
            },
        }
    }
}

impl<'a> Interpreter for ColumnSpecs<'a> {
    type Summary = ColumnSpec<'a>;

    fn eval_column(&self, id: usize) -> Self::Summary {
        self.columns[id].clone()
    }

    fn eval_literal(
        &self,
        result: &Result<Row, EvalError>,
        col_type: &ColumnType,
    ) -> Self::Summary {
        let col_type = col_type.clone();
        let range = self.eval_result(result.as_ref().map(|row| {
            self.arena
                .make_datum(|packer| packer.push(row.unpack_first()))
        }));
        ColumnSpec { col_type, range }
    }

    fn eval_unmaterializable(&self, func: &UnmaterializableFunc) -> Self::Summary {
        let col_type = func.output_type();
        let range = self
            .unmaterializables
            .get(func)
            .copied()
            .unwrap_or(ResultSpec::has_type(&func.output_type(), false));
        ColumnSpec { col_type, range }
    }

    fn eval_unary(&self, func: &UnaryFunc, summary: Self::Summary) -> Self::Summary {
        let is_monotonic = unary_monotonic(func);
        let expr = MirScalarExpr::CallUnary {
            func: func.clone(),
            expr: Box::new(MirScalarExpr::Column(0)),
        };
        let col_type = func.output_type(summary.col_type);
        let range = summary
            .range
            .flat_map(is_monotonic, |datum| {
                self.eval_result(expr.eval(&[datum], self.arena))
            })
            .intersect(ResultSpec::has_type(&col_type, true));

        ColumnSpec { col_type, range }
    }

    fn eval_binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary {
        let (left_monotonic, right_monotonic) = binary_monotonic(func);
        let expr = MirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(MirScalarExpr::Column(0)),
            expr2: Box::new(MirScalarExpr::Column(1)),
        };
        let col_type = func.output_type(left.col_type, right.col_type);
        let range = left
            .range
            .flat_map(left_monotonic, |left| {
                right.range.flat_map(right_monotonic, |right| {
                    self.eval_result(expr.eval(&[left, right], self.arena))
                })
            })
            .intersect(ResultSpec::has_type(&col_type, true));
        ColumnSpec { col_type, range }
    }

    fn eval_variadic(&self, func: &VariadicFunc, args: Vec<Self::Summary>) -> Self::Summary {
        fn eval_loop<'a>(
            is_monotonic: Monotonic,
            stack: &mut Vec<Datum<'a>>,
            remaining: &[ResultSpec<'a>],
            datum_map: &mut impl FnMut(&[Datum<'a>]) -> ResultSpec<'a>,
        ) -> ResultSpec<'a> {
            if remaining.is_empty() {
                datum_map(stack)
            } else {
                remaining[0].flat_map(is_monotonic, |datum| {
                    stack.push(datum);
                    let result = eval_loop(is_monotonic, stack, &remaining[1..], datum_map);
                    stack.pop();
                    result
                })
            }
        }
        let fn_expr = MirScalarExpr::CallVariadic {
            func: func.clone(),
            exprs: (0..args.len()).map(MirScalarExpr::Column).collect(),
        };
        let mut col_types = vec![];
        let mut ranges = vec![];
        for ColumnSpec { col_type, range } in args {
            col_types.push(col_type);
            ranges.push(range);
        }

        let col_type = func.output_type(col_types);

        let mut stack = vec![];
        let range = eval_loop(
            variadic_monotonic(func),
            &mut stack,
            &ranges,
            &mut |datums| self.eval_result(fn_expr.eval(datums, self.arena)),
        )
        .intersect(ResultSpec::has_type(&col_type, false));

        ColumnSpec { col_type, range }
    }

    fn eval_if(
        &self,
        cond: Self::Summary,
        then: Self::Summary,
        els: Self::Summary,
    ) -> Self::Summary {
        let col_type = ColumnType {
            scalar_type: then.col_type.scalar_type,
            nullable: then.col_type.nullable || els.col_type.nullable,
        };

        let range = cond
            .range
            .flat_map(Monotonic::Yes, |datum| match datum {
                Datum::True => then.range,
                Datum::False => els.range,
                _ => ResultSpec::fails(),
            })
            .intersect(ResultSpec::has_type(&col_type, true));

        ColumnSpec { col_type, range }
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, RowArena, ScalarType};

    use crate::func::AbsInt64;
    use crate::{BinaryFunc, MirScalarExpr, UnaryFunc};

    use super::*;

    #[test]
    fn test_eval_range() {
        // Example inspired by the tumbling windows temporal filter in the docs
        let period_ms = MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[Datum::Int64(10)])),
            ScalarType::UInt64.nullable(false),
        );
        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::Gte,
            expr1: Box::new(MirScalarExpr::CallUnmaterializable(
                UnmaterializableFunc::MzNow,
            )),
            expr2: Box::new(MirScalarExpr::CallBinary {
                func: BinaryFunc::MulInt64,
                expr1: Box::new(period_ms.clone()),
                expr2: Box::new(MirScalarExpr::CallBinary {
                    func: BinaryFunc::DivInt64,
                    expr1: Box::new(MirScalarExpr::Column(0)),
                    expr2: Box::new(period_ms),
                }),
            }),
        };

        {
            // Non-overlapping windows
            let arena = RowArena::new();
            let mut interpreter = ColumnSpecs::new(
                RelationType::new(vec![ScalarType::UInt64.nullable(false)]),
                &arena,
            );
            interpreter.push_unmaterializable(
                UnmaterializableFunc::MzNow,
                ResultSpec::value_between(10i64.into(), 20i64.into()),
            );
            interpreter.push_column(0, ResultSpec::value_between(30i64.into(), 40i64.into()));

            let range_out = interpreter.eval_expr(&expr).range;
            assert!(range_out.may_contain(Datum::False));
            assert!(!range_out.may_contain(Datum::True));
            assert!(!range_out.may_contain(Datum::Null));
            assert!(!range_out.may_fail());
        }

        {
            // Overlapping windows
            let arena = RowArena::new();
            let mut interpreter = ColumnSpecs::new(
                RelationType::new(vec![ScalarType::UInt64.nullable(false)]),
                &arena,
            );
            interpreter.push_unmaterializable(
                UnmaterializableFunc::MzNow,
                ResultSpec::value_between(10i64.into(), 35i64.into()),
            );
            interpreter.push_column(0, ResultSpec::value_between(30i64.into(), 40i64.into()));

            let range_out = interpreter.eval_expr(&expr).range;
            assert!(range_out.may_contain(Datum::False));
            assert!(range_out.may_contain(Datum::True));
            assert!(!range_out.may_contain(Datum::Null));
            assert!(!range_out.may_fail());
        }
    }

    #[test]
    fn test_trace() {
        use super::Trace;

        let _input_type = ScalarType::UInt64.nullable(false);
        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::Gte,
            expr1: Box::new(MirScalarExpr::Column(0)),
            expr2: Box::new(MirScalarExpr::CallBinary {
                func: BinaryFunc::AddInt64,
                expr1: Box::new(MirScalarExpr::Column(1)),
                expr2: Box::new(MirScalarExpr::CallUnary {
                    func: UnaryFunc::AbsInt64(AbsInt64),
                    expr: Box::new(MirScalarExpr::Column(3)),
                }),
            }),
        };
        let trace = Trace.eval_expr(&expr);
        assert_eq!(
            trace,
            vec![
                Pushdownable::Yes, // one side of a conditional
                Pushdownable::Yes, // other side of the conditional, nested in +
                Pushdownable::No,  // not referenced!
                Pushdownable::No   // referenced, but we can't see through abs()
            ]
        )
    }
}
