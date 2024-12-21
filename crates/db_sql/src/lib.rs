use db_common::{Aggregation, AggregationType, DbError, ExecutionTree, Filter, FilterOperator, QueryPlan, QueryResult};
use sqlparser::{ast::{Statement, Query, Expr, BinaryOperator, Ident, Function, SelectItem, FunctionArg, FunctionArgExpr}, dialect::GenericDialect, parser::Parser};
pub struct SqlParser;

impl SqlParser {
    pub fn parse_sql(sql: &str) -> Result<Query, DbError> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DbError::SqlParsingError(e.to_string()))?;
        
        match ast.first() {
            Some(Statement::Query(query)) => Ok(*query.clone()),
            _ => Err(DbError::SqlParsingError("Invalid SQL statement".to_string())),
        }
    }
    pub fn transform_sql(query: Query) -> Result<ExecutionTree, DbError> {
       // Transform parsed SQL to an internal execution tree
       let select = query.body;

       let mut query_plan = QueryPlan {
           chunks: Vec::new(),
           column_name: None,
           filter: None,
           aggregation: None,
           projection: Vec::new()
       };
      if let sqlparser::ast::SetExpr::Select(select) = *select {
           for projection in &select.projection {
                match projection {
                     SelectItem::UnnamedExpr(expression)  => {
                        match expression {
                            Expr::Identifier(identifier) => query_plan.projection.push(identifier.value.clone()),
                            //   Expr::Function(function) => {
                            //     let aggregation = Self::transform_aggregate(function.clone())?;
                            //      query_plan.aggregation = Some(aggregation);
                            // },
                             _ => return Err(DbError::SqlParsingError("Invalid projection expression".to_string()))
                        }
                      },
                      SelectItem::Wildcard(_) => {
                         //For now we will get all columns
                        query_plan.projection.push("*".to_string());
                     }
                      _ => return Err(DbError::SqlParsingError("Invalid projection expression".to_string()))
                 }
           }

          if let Some(filter_expression) = select.selection {
               let filter = Self::transform_where(filter_expression)?;
               query_plan.filter = filter;
           }
       }
        Ok(ExecutionTree::Select{plan: query_plan})
    }

    // fn transform_aggregate(function: Function) -> Result<Aggregation, DbError> {
    //     let aggregation_type = match function.name.to_string().to_lowercase().as_str() {
    //         "sum" => AggregationType::Sum,
    //         "avg" => AggregationType::Average,
    //         _ => return Err(DbError::SqlParsingError("Invalid aggregation function".to_string())),
    //     };

    //     let column_index = function
    //         .args.
    //         .find_map(|arg| Ok(match arg {
    //             FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => match expression {
    //                 Expr::Identifier(identifier) => Some(
    //                     identifier
    //                         .value
    //                         .parse::<usize>()
    //                         .map_err(|_| DbError::SqlParsingError("Column name must be an index".to_string()))?,
    //                 ),
    //                 _ => return Err(DbError::SqlParsingError("Invalid column name".to_string())),
    //             },
    //             _ => return Err(DbError::SqlParsingError("Invalid aggregation function argument".to_string())),
    //         }))
    //         .ok_or_else(|| DbError::SqlParsingError("Invalid aggregation function".to_string()))?;

    //     Ok(Aggregation {
    //         column_index,
    //         aggregation_type,
    //     })
    // }


    fn transform_where(expression: Expr) -> Result<Option<Filter>, DbError> {
        if let Expr::BinaryOp { left, op, right } = expression {
            let column_name = match *left {
                Expr::Identifier(identifier) => {
                    identifier.value.clone()
                },
                _ => return Err(DbError::SqlParsingError("Invalid column name".to_string())),
            };

             let value = match *right {
                Expr::Value(value) => {
                     value.to_string()
                },
                _ => return Err(DbError::SqlParsingError("Invalid value".to_string())),
             };
            
             let operator = match op {
                BinaryOperator::Gt => FilterOperator::GreaterThan,
                BinaryOperator::Lt => FilterOperator::LessThan,
                BinaryOperator::Eq => FilterOperator::Equal,
                BinaryOperator::NotEq => FilterOperator::NotEqual,
                _ => return Err(DbError::SqlParsingError("Invalid operator".to_string()))
             };
           return Ok(Some(Filter{ column_index: 0, value, operator, column_name}))
        }
      Ok(None)
    }
}

