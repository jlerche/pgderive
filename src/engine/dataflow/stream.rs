use anyhow::{Context, Result, ensure};
use std::{any::Any, marker::PhantomData, sync::Arc};

pub(super) type Value = Arc<dyn Any + Send + Sync>;
pub(super) type Values = Vec<Option<Value>>;
type Select<T> = dyn Fn(&Values) -> Result<T> + Send + Sync;
type Invariant<T> = fn(T) -> T;

/// Typed edge carrying successive values of T across logical circuit ticks.
///
/// A handle identifies an edge; it does not contain a batch or advance a clock.
/// Handles are invariant in T, preserving exact type identity through shared storage.
///
/// Rust rejects connections between incompatible edge and callback input types:
///
/// ```compile_fail
/// use pgderive::engine::dataflow::{CircuitBuilder, NodeContext, NodeOutput, Stream, TimedBatch};
/// use std::sync::Arc;
/// fn incompatible<S: Send + Sync + 'static>(builder: &mut CircuitBuilder<S>, edge: &Stream<String>) {
///     let _result = builder.unary("project", edge,
///         |_: NodeContext<S>, _: TimedBatch<Arc<u64>>| async { Ok(NodeOutput::pure(0_u64)) });
/// }
/// ```
///
/// Function-pointer subtyping must not change an edge's stored type identity:
///
/// ```compile_fail
/// use pgderive::engine::dataflow::Stream;
/// fn change_type(edge: Stream<fn(&str)>) -> Stream<fn(&'static str)> { edge }
/// ```
pub struct Stream<T> {
    pub(super) owner: Arc<()>,
    pub(super) index: usize,
    pub(super) source: bool,
    marker: PhantomData<Invariant<T>>,
}
impl<T> Clone for Stream<T> {
    fn clone(&self) -> Self {
        Self::new(self.owner.clone(), self.index, self.source)
    }
}
impl<T> Stream<T> {
    pub(super) const fn new(owner: Arc<()>, index: usize, source: bool) -> Self {
        Self { owner, index, source, marker: PhantomData }
    }
}
impl<T: Clone + Send + Sync + 'static> Stream<T> {
    /// Select this edge as a visible output, cloning only its final tick value.
    #[must_use]
    pub fn output(&self) -> Output<T> {
        let index = self.index;
        Output {
            owner: self.owner.clone(),
            indices: vec![index],
            select: Arc::new(move |values| Ok((*read::<T>(values, index)?).clone())),
        }
    }
}

/// Typed selection of one or more visible circuit output edges.
pub struct Output<T> {
    pub(super) owner: Arc<()>,
    pub(super) indices: Vec<usize>,
    pub(super) select: Arc<Select<T>>,
}
impl<T: Send + Sync + 'static> Output<T> {
    /// Select heterogeneous outputs as a tuple without adding an operator node.
    ///
    /// # Errors
    /// Rejects outputs from different circuit builders.
    pub fn zip<U: Send + Sync + 'static>(self, other: Output<U>) -> Result<Output<(T, U)>> {
        ensure!(Arc::ptr_eq(&self.owner, &other.owner), "foreign output edge");
        let mut indices = self.indices;
        indices.extend(other.indices);
        Ok(Output {
            owner: self.owner,
            indices,
            select: Arc::new(move |values| Ok(((self.select)(values)?, (other.select)(values)?))),
        })
    }
}

/// Complete synchronized source values for one circuit tick.
/// Every source must be supplied explicitly, including empty delta batches.
pub struct CircuitInputs {
    pub(super) owner: Arc<()>,
    pub(super) values: Values,
}
impl CircuitInputs {
    /// Supply one typed source delta. Values are shared immutably during evaluation.
    ///
    /// # Errors
    /// Rejects foreign, nonsource, duplicate or invalid source handles.
    pub fn insert<T: Send + Sync + 'static>(&mut self, source: &Stream<T>, value: T) -> Result<()> {
        ensure!(
            Arc::ptr_eq(&self.owner, &source.owner) && source.source,
            "foreign or nonsource input"
        );
        let slot = self.values.get_mut(source.index).context("invalid source index")?;
        ensure!(slot.is_none(), "duplicate source input");
        *slot = Some(Arc::new(value));
        Ok(())
    }
}
pub(super) fn read<T: Send + Sync + 'static>(values: &Values, index: usize) -> Result<Arc<T>> {
    let value = values.get(index).and_then(Option::as_ref).context("missing stream value")?;
    Arc::downcast::<T>(value.clone()).map_err(|_| anyhow::anyhow!("stream value type mismatch"))
}
