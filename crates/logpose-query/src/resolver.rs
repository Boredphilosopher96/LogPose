//! [`QueryResolver`]: the [`RowSetResolver`] the engine calls for delete-by-filter and
//! update-by-filter.

use crate::ops::resolve_view;
use logpose_storage::{BoxFuture, ReadView, RowSetResolver};
use logpose_types::{Result, UnitId, filter::FilterExpr};
use roaring::RoaringBitmap;
use std::sync::Arc;

/// Resolves filters with this crate's compiler and per-unit evaluation.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryResolver;

impl RowSetResolver for QueryResolver {
    fn resolve<'a>(
        &'a self,
        view: &'a ReadView,
        filter: &'a FilterExpr,
    ) -> BoxFuture<'a, Result<Vec<(UnitId, RoaringBitmap)>>> {
        Box::pin(async move { resolve_view(view, filter).await.map_err(Into::into) })
    }
}

/// The resolver to inject as `EngineConfig::resolver`.
#[must_use]
pub fn resolver() -> Arc<dyn RowSetResolver> {
    Arc::new(QueryResolver)
}
