use db_common::{DbError, ExecutionTree, Filter, FilterOperator, QueryPlan, QueryResult};
use sqlparser::{ast::{Statement, Query, Expr, BinaryOperator}, dialect::GenericDialect, parser::Parser};
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

       if let sqlparser::ast::SetExpr::Select(select) = *select {
           let mut filter = None;
             let mut column_name = None;
           if let Some(filter_expression) = select.selection {
              filter = Self::transform_where(filter_expression)?;
           }
           if let Some(projection) = select.projection.first() {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = projection {
                    if let Expr::Identifier(identifier) = expr {
                       column_name = Some(identifier.value.clone());
                    }
               }
            }

           return Ok(ExecutionTree::Select {plan: QueryPlan { chunks: Vec::new(), filter, column_name}})
       }
        Ok(ExecutionTree::Select {plan: QueryPlan { chunks: Vec::new(), filter: None, column_name: None}})
    }

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
           return Ok(Some(Filter{ column_index: 0, value, operator}))
        }
      Ok(None)
    }
}



//Placeholder function to actually execute query on some data.
pub async fn execute(_tree: ExecutionTree) -> Result<QueryResult, DbError> {
    //TODO
    Ok(QueryResult{ rows: Vec::new()})
}