//! The filter (predicate) AST shared by the query crate and the storage
//! writer.
//!
//! [`FilterExpr`] lives here, not in `logpose-query`, so the storage writer
//! can carry delete-by-filter and update-by-filter requests without
//! depending on the query crate. Evaluation, validation, and planning stay in
//! `logpose-query`.
//!
//! The JSON shape is tagged by `kind`:
//!
//! ```json
//! { "kind": "and", "children": [
//!   { "kind": "comparison", "field": "color", "operator": "eq", "value": "red" },
//!   { "kind": "not", "child": { "kind": "comparison", "field": "size", "operator": "exists" } }
//! ] }
//! ```

use crate::ScalarMetadataValue;
use serde::{Deserialize, Serialize};

/// A boolean filter over record fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FilterExpr {
    /// Conjunction over child filters.
    And {
        /// Child filters.
        children: Vec<FilterExpr>,
    },
    /// Disjunction over child filters.
    Or {
        /// Child filters.
        children: Vec<FilterExpr>,
    },
    /// Negation of a child filter.
    Not {
        /// Child filter.
        child: Box<FilterExpr>,
    },
    /// Comparison of one top-level field against a value.
    Comparison(FilterComparison),
}

/// One field comparison inside a [`FilterExpr`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterComparison {
    /// Target top-level field.
    pub field: String,
    /// Comparison operator.
    pub operator: FilterOperator,
    /// Scalar operand, for operators that take one.
    #[serde(default)]
    pub value: Option<ScalarMetadataValue>,
}

/// Operator of a [`FilterComparison`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    /// Exact scalar equality.
    Eq,
    /// Scalar inequality.
    Ne,
    /// Strictly less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Strictly greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// The field is present.
    Exists,
    /// The field is present and null.
    IsNull,
}

#[cfg(test)]
mod tests {
    use super::{FilterComparison, FilterExpr, FilterOperator};
    use crate::ScalarMetadataValue;
    use serde_json::json;

    #[test]
    fn filter_json_shape_is_tagged_by_kind() {
        let filter = FilterExpr::And {
            children: vec![
                FilterExpr::Comparison(FilterComparison {
                    field: "color".to_owned(),
                    operator: FilterOperator::Eq,
                    value: Some(ScalarMetadataValue::String("red".to_owned())),
                }),
                FilterExpr::Not {
                    child: Box::new(FilterExpr::Comparison(FilterComparison {
                        field: "size".to_owned(),
                        operator: FilterOperator::IsNull,
                        value: None,
                    })),
                },
            ],
        };
        let encoded = serde_json::to_value(&filter).expect("filter should serialize");
        assert_eq!(
            encoded,
            json!({
                "kind": "and",
                "children": [
                    { "kind": "comparison", "field": "color", "operator": "eq", "value": "red" },
                    { "kind": "not", "child": {
                        "kind": "comparison", "field": "size", "operator": "is_null", "value": null
                    } }
                ]
            })
        );
        let decoded: FilterExpr = serde_json::from_value(encoded).expect("filter should parse");
        assert_eq!(decoded, filter);
    }

    #[test]
    fn comparison_value_defaults_to_none() {
        let decoded: FilterExpr = serde_json::from_value(json!({
            "kind": "comparison", "field": "size", "operator": "exists"
        }))
        .expect("filter should parse");
        assert_eq!(
            decoded,
            FilterExpr::Comparison(FilterComparison {
                field: "size".to_owned(),
                operator: FilterOperator::Exists,
                value: None,
            })
        );
    }
}
