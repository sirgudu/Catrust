// =============================================================================
// SQL PLANNER — Traduction des requêtes CQL optimisées en SQL
// =============================================================================
//
// Ce module traduit une CqlQuery (potentiellement optimisée par le
// PathOptimizer) en requêtes SQL concrètes.
//
// Le gain principal : un chemin catégorique de longueur N se traduit
// en N-1 JOINs. Grâce à l'optimiseur, N est réduit, donc moins de JOINs.
//
// ┌──────────────────────────────────────────────────────────────────┐
// │ CQL Query :                                                      │
// │   from e : Employee                                              │
// │   where e.department.manager.emp_name = "Alice"                  │
// │   return e.emp_name, e.department.dept_name                      │
// │                                                                  │
// │ SANS optimisation (3 JOINs) :                                    │
// │   SELECT e.emp_name, d.dept_name                                 │
// │   FROM Employee e                                                │
// │   JOIN Department d ON e.department = d.catrust_id               │
// │   JOIN Employee mgr ON d.manager = mgr.catrust_id               │
// │   WHERE mgr.emp_name = 'Alice'                                   │
// │                                                                  │
// │ AVEC optimisation (2 JOINs, via path equation) :                 │
// │   SELECT e.emp_name, d.dept_name                                 │
// │   FROM Employee e                                                │
// │   JOIN Department d ON e.department = d.catrust_id               │
// │   WHERE e.direct_mgr_name = 'Alice'  -- shortcut !              │
// │                                                                  │
// └──────────────────────────────────────────────────────────────────┘
//
// =============================================================================

use crate::core::schema::{Schema, Edge, Path};
use crate::core::query::{CqlQuery, QueryBlock, WhereClause, AttributeBinding};
use crate::core::optimize::PathOptimizer;
use crate::backend::sql::SqlDialect;

/// Résultat de la planification SQL
#[derive(Debug, Clone)]
pub struct SqlPlan {
    /// La requête SQL générée
    pub sql: String,
    /// Nombre de JOINs dans la requête
    pub join_count: usize,
    /// Nombre de JOINs éliminés par l'optimisation
    pub joins_saved: usize,
    /// Explication de l'optimisation
    pub explanation: Vec<String>,
}

impl std::fmt::Display for SqlPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.sql)?;
        writeln!(f, "-- {} JOINs ({} éliminés par optimisation catégorique)", 
                 self.join_count, self.joins_saved)?;
        for exp in &self.explanation {
            writeln!(f, "-- {}", exp)?;
        }
        Ok(())
    }
}

/// Planificateur SQL : traduit une CqlQuery en SQL optimisé.
pub struct SqlPlanner<'a, D: SqlDialect> {
    dialect: &'a D,
    schema: &'a Schema,
    optimizer: PathOptimizer,
}

impl<'a, D: SqlDialect> SqlPlanner<'a, D> {
    pub fn new(dialect: &'a D, schema: &'a Schema) -> Self {
        let optimizer = PathOptimizer::from_schema(schema);
        SqlPlanner { dialect, schema, optimizer }
    }

    /// Planifie une requête CQL complète (tous les blocks)
    pub fn plan_query(&self, query: &CqlQuery) -> Vec<SqlPlan> {
        query.blocks.iter().map(|block| self.plan_block(block)).collect()
    }

    /// Planifie un seul block de requête
    fn plan_block(&self, block: &QueryBlock) -> SqlPlan {
        let mut joins: Vec<JoinClause> = Vec::new();
        let mut where_parts: Vec<String> = Vec::new();
        let mut select_parts: Vec<String> = Vec::new();
        let mut explanation: Vec<String> = Vec::new();
        let mut alias_counter: usize = 0;
        let mut joins_saved = 0;

        // Table principale (première variable FROM)
        let (main_var, main_entity) = block.from_vars.iter().next().unwrap();
        let main_alias = main_var.clone();

        // Collecter les SELECT
        for (result_attr, binding) in &block.attribute_bindings {
            let (alias, attr, extra_joins, saved) = self.resolve_attribute_binding(
                binding, &main_alias, block, &mut alias_counter,
            );
            select_parts.push(format!("{}.{} AS {}",
                alias,
                self.dialect.quote_identifier(&attr),
                self.dialect.quote_identifier(result_attr),
            ));
            joins.extend(extra_joins);
            joins_saved += saved;
        }

        if select_parts.is_empty() {
            select_parts.push(format!("{}.*", main_alias));
        }

        // Collecter les WHERE
        for clause in &block.where_clauses {
            match clause {
                WhereClause::Comparison { var, path, op, value } => {
                    let (resolved, extra_joins, saved) = self.resolve_path_to_sql(
                        var, block, path, &mut alias_counter,
                    );
                    where_parts.push(format!("{} {} {}", resolved, op, value_to_sql_literal(value)));
                    joins.extend(extra_joins);
                    joins_saved += saved;

                    if saved > 0 {
                        explanation.push(format!(
                            "Path equation raccourcit {}.{} → {} JOIN(s) éliminé(s)",
                            var, path.join("."), saved
                        ));
                    }
                }
                WhereClause::PathEqual { var1, path1, var2, path2 } => {
                    let (r1, j1, s1) = self.resolve_path_to_sql(var1, block, path1, &mut alias_counter);
                    let (r2, j2, s2) = self.resolve_path_to_sql(var2, block, path2, &mut alias_counter);
                    where_parts.push(format!("{} = {}", r1, r2));
                    joins.extend(j1);
                    joins.extend(j2);
                    joins_saved += s1 + s2;
                }
            }
        }

        // Dédupliquer les JOINs
        joins.sort_by(|a, b| a.alias.cmp(&b.alias));
        joins.dedup_by(|a, b| a.alias == b.alias);

        // Assembler le SQL
        let mut sql = format!("SELECT {}\nFROM {} {}",
            select_parts.join(", "),
            self.dialect.quote_identifier(main_entity),
            main_alias,
        );

        for join in &joins {
            sql.push_str(&format!("\nJOIN {} {} ON {}.{} = {}.catrust_id",
                self.dialect.quote_identifier(&join.table),
                join.alias,
                join.source_alias,
                self.dialect.quote_identifier(&join.fk_column),
                join.alias,
            ));
        }

        if !where_parts.is_empty() {
            sql.push_str(&format!("\nWHERE {}", where_parts.join("\n  AND ")));
        }

        sql.push(';');

        let join_count = joins.len();

        SqlPlan {
            sql,
            join_count,
            joins_saved,
            explanation,
        }
    }

    /// Résout un chemin catégorique en expression SQL + JOINs nécessaires.
    ///
    /// Optimise d'abord le chemin via les path equations, puis génère les JOINs.
    fn resolve_path_to_sql(
        &self,
        var: &str,
        block: &QueryBlock,
        path: &[String],
        alias_counter: &mut usize,
    ) -> (String, Vec<JoinClause>, usize) {
        let entity = block.from_vars.get(var).unwrap();

        // Séparer le chemin en FK + attribut final
        let (fk_path, final_attr) = if path.is_empty() {
            return (format!("{}.catrust_id", var), vec![], 0);
        } else {
            // Le dernier élément est-il un attribut ou une FK ?
            let last = path.last().unwrap();
            if let Some(Edge::Attribute { .. }) = self.schema.edges.get(last) {
                (&path[..path.len()-1], Some(last.as_str()))
            } else {
                (path, None)
            }
        };

        // Optimiser le chemin de FK
        let original_len = fk_path.len();
        let optimized_fk = if !fk_path.is_empty() {
            let full = Path {
                start: entity.clone(),
                edges: fk_path.to_vec(),
            };
            let opt = self.optimizer.optimize(&full);
            opt.optimized.edges
        } else {
            vec![]
        };
        let saved = if original_len > optimized_fk.len() {
            original_len - optimized_fk.len()
        } else {
            0
        };

        // Générer les JOINs pour le chemin optimisé
        let mut joins = Vec::new();
        let mut current_alias = var.to_string();

        for fk_name in &optimized_fk {
            if let Some(Edge::ForeignKey { target, .. }) = self.schema.edges.get(fk_name) {
                *alias_counter += 1;
                let new_alias = format!("j{}", alias_counter);
                joins.push(JoinClause {
                    table: target.clone(),
                    alias: new_alias.clone(),
                    source_alias: current_alias.clone(),
                    fk_column: fk_name.clone(),
                });
                current_alias = new_alias;
            }
        }

        // Expression finale
        let sql_expr = match final_attr {
            Some(attr) => format!("{}.{}", current_alias, self.dialect.quote_identifier(attr)),
            None => format!("{}.catrust_id", current_alias),
        };

        (sql_expr, joins, saved)
    }

    /// Résout un AttributeBinding en (alias, attribut, joins, joins_saved)
    fn resolve_attribute_binding(
        &self,
        binding: &AttributeBinding,
        _main_alias: &str,
        block: &QueryBlock,
        alias_counter: &mut usize,
    ) -> (String, String, Vec<JoinClause>, usize) {
        let mut full_path = binding.path.clone();
        full_path.push(binding.attribute.clone());

        let (sql_expr, joins, saved) = self.resolve_path_to_sql(
            &binding.from_var, block, &full_path, alias_counter,
        );

        // Extraire alias et attribut de l'expression "alias.attr"
        let parts: Vec<&str> = sql_expr.splitn(2, '.').collect();
        if parts.len() == 2 {
            let attr = parts[1].trim_matches('"').to_string();
            (parts[0].to_string(), attr, joins, saved)
        } else {
            (sql_expr, String::new(), joins, saved)
        }
    }
}

/// Un JOIN à ajouter à la requête
#[derive(Debug, Clone)]
struct JoinClause {
    table: String,
    alias: String,
    source_alias: String,
    fk_column: String,
}

/// Convertit une Value en littéral SQL
fn value_to_sql_literal(value: &crate::core::typeside::Value) -> String {
    use crate::core::typeside::Value;
    match value {
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Integer(i) => format!("{}", i),
        Value::Float(f) => format!("{}", f),
        Value::Boolean(b) => if *b { "TRUE".into() } else { "FALSE".into() },
        Value::Null => "NULL".into(),
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::{BaseType, Value};
    use crate::core::query::{CqlQuery, QueryBlock, WhereClause, CompOp, AttributeBinding};
    use crate::backend::sql::PostgresDialect;
    use std::collections::HashMap;

    fn company_schema() -> Schema {
        let mut s = Schema::new("Company");
        s.add_node("Employee")
         .add_node("Department")
         .add_fk("department", "Employee", "Department")
         .add_fk("manager", "Department", "Employee")
         .add_fk("direct_mgr", "Employee", "Employee")
         .add_attribute("emp_name", "Employee", BaseType::String)
         .add_attribute("salary", "Employee", BaseType::Integer)
         .add_attribute("dept_name", "Department", BaseType::String)
         .add_path_equation(
             Path::new("Employee", vec!["department", "manager"]),
             Path::new("Employee", vec!["direct_mgr"]),
         );
        s
    }

    #[test]
    fn test_simple_query_plan() {
        let schema = company_schema();
        let planner = SqlPlanner::new(&PostgresDialect, &schema);

        let mut query = CqlQuery::new("FindEngineers", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    path: vec!["department".into(), "dept_name".into()],
                    op: CompOp::Eq,
                    value: Value::String("Engineering".into()),
                },
            ],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(),
                    path: vec![],
                    attribute: "emp_name".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let plans = planner.plan_query(&query);
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];

        println!("=== Requête SQL simple ===\n{}", plan);
        assert!(plan.sql.contains("JOIN"));
        assert!(plan.sql.contains("dept_name"));
    }

    #[test]
    fn test_optimized_query_plan() {
        let schema = company_schema();
        let planner = SqlPlanner::new(&PostgresDialect, &schema);

        let mut query = CqlQuery::new("FindByManager", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    // Chemin LONG : e.department.manager.emp_name (3 étapes, 2 JOINs)
                    // Grâce à path equation department.manager = direct_mgr :
                    // → Optimisé en : e.direct_mgr.emp_name (2 étapes, 1 JOIN)
                    path: vec!["department".into(), "manager".into(), "emp_name".into()],
                    op: CompOp::Eq,
                    value: Value::String("Alice".into()),
                },
            ],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(),
                    path: vec![],
                    attribute: "emp_name".into(),
                }),
                ("salary".into(), AttributeBinding {
                    from_var: "e".into(),
                    path: vec![],
                    attribute: "salary".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let plans = planner.plan_query(&query);
        let plan = &plans[0];

        println!("=== Requête SQL OPTIMISÉE ===\n{}", plan);
        assert!(plan.joins_saved > 0, "Devrait avoir éliminé au moins 1 JOIN");
        assert!(plan.sql.contains("direct_mgr"), "Devrait utiliser le raccourci direct_mgr");
        // Ne devrait PAS contenir 2 JOINs chaînés department puis manager
        println!("JOINs : {}, JOINs éliminés : {}", plan.join_count, plan.joins_saved);
    }

    #[test]
    fn test_no_optimization_needed() {
        // Schéma SANS path equations
        let mut schema = Schema::new("Simple");
        schema.add_node("A").add_node("B")
              .add_fk("f", "A", "B")
              .add_attribute("name", "B", BaseType::String);

        let planner = SqlPlanner::new(&PostgresDialect, &schema);

        let mut query = CqlQuery::new("Q", "Simple");
        query.add_block(QueryBlock {
            target_entity: "Res".into(),
            from_vars: HashMap::from([("a".into(), "A".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "a".into(),
                    path: vec!["f".into(), "name".into()],
                    op: CompOp::Eq,
                    value: Value::String("test".into()),
                },
            ],
            attribute_bindings: HashMap::new(),
            fk_bindings: HashMap::new(),
        });

        let plans = planner.plan_query(&query);
        let plan = &plans[0];
        assert_eq!(plan.joins_saved, 0);
        println!("=== Sans optimisation ===\n{}", plan);
    }
}
