// =============================================================================
// QUERY — Requêtes catégoriques (composition Δ ∘ Σ)
// =============================================================================
//
// En CQL, une REQUÊTE Q : S → T est définie par :
//   1. Un schéma intermédiaire ("freeze") pour chaque entité cible
//   2. Des blocs FROM/WHERE qui spécifient les données à sélectionner
//
// Mais fondamentalement, toute requête CQL se décompose en :
//   Q = Σ_G ∘ Δ_F   (d'abord un pullback, puis un pushforward)
//
// En SQL, ça donne :
//   Δ_F = la partie SELECT ... FROM ... JOIN ... WHERE (sélection)
//   Σ_G = la partie INSERT INTO ... (structuration du résultat)
//
// Ce module définit les requêtes et leur optimisation via le PathOptimizer.
//
// =============================================================================

use std::collections::HashMap;
use super::schema::{Schema, Path};
use super::typeside::Value;

/// Un bloc FROM d'une requête CQL : pour une entité cible,
/// quelles entités source et quels chemins utiliser.
///
/// Exemple CQL :
/// ```cql
/// query FindSeniorEngineers : Company -> Results = {
///     entity SeniorEng -> {
///         from e : Employee
///         where e.department.dept_name = "Engineering"
///         where e.salary > 80000
///         attributes
///             name -> e.emp_name
///             dept -> e.department.dept_name
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct QueryBlock {
    /// Nom de l'entité cible dans le schéma résultat
    pub target_entity: String,
    /// Variables FROM : var_name → entity_name dans le schéma source
    pub from_vars: HashMap<String, String>,
    /// Conditions WHERE : (chemin, opérateur, valeur)
    pub where_clauses: Vec<WhereClause>,
    /// Projection des attributs : attr_résultat → chemin depuis une variable FROM
    pub attribute_bindings: HashMap<String, AttributeBinding>,
    /// Projection des FK : fk_résultat → chemin depuis une variable FROM
    pub fk_bindings: HashMap<String, FkBinding>,
}

/// Une clause WHERE
#[derive(Debug, Clone)]
pub enum WhereClause {
    /// Comparaison : chemin op valeur (ex: e.salary > 80000)
    Comparison {
        var: String,           // variable FROM
        path: Vec<String>,     // chemin de FK puis attribut
        op: CompOp,            // opérateur
        value: Value,          // valeur constante
    },
    /// Égalité de chemins : deux chemins doivent mener au même endroit
    /// (ex: e1.department = e2.department)
    PathEqual {
        var1: String,
        path1: Vec<String>,
        var2: String,
        path2: Vec<String>,
    },
}

/// Opérateur de comparaison
#[derive(Debug, Clone, PartialEq)]
pub enum CompOp {
    Eq,      // =
    Neq,     // !=
    Lt,      // <
    Gt,      // >
    Lte,     // <=
    Gte,     // >=
}

impl std::fmt::Display for CompOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompOp::Eq => write!(f, "="),
            CompOp::Neq => write!(f, "!="),
            CompOp::Lt => write!(f, "<"),
            CompOp::Gt => write!(f, ">"),
            CompOp::Lte => write!(f, "<="),
            CompOp::Gte => write!(f, ">="),
        }
    }
}

/// Binding d'un attribut résultat vers un chemin source
#[derive(Debug, Clone)]
pub struct AttributeBinding {
    pub from_var: String,        // variable FROM
    pub path: Vec<String>,       // chemin de FK (peut être vide)
    pub attribute: String,       // attribut final
}

/// Binding d'une FK résultat vers un chemin source
#[derive(Debug, Clone)]
pub struct FkBinding {
    pub from_var: String,
    pub path: Vec<String>,
}

/// Une requête CQL complète : schéma source → schéma résultat
#[derive(Debug, Clone)]
pub struct CqlQuery {
    pub name: String,
    pub source_schema_name: String,
    pub result_schema: Schema,
    pub blocks: Vec<QueryBlock>,
}

impl CqlQuery {
    pub fn new(name: &str, source_schema: &str) -> Self {
        CqlQuery {
            name: name.to_string(),
            source_schema_name: source_schema.to_string(),
            result_schema: Schema::new(&format!("{}_result", name)),
            blocks: Vec::new(),
        }
    }

    /// Ajoute un block de requête
    pub fn add_block(&mut self, block: QueryBlock) {
        // Ajouter l'entité au schéma résultat
        self.result_schema.add_node(&block.target_entity);
        self.blocks.push(block);
    }

    /// Optimise les chemins de la requête en utilisant les path equations du schéma source.
    pub fn optimize(&self, source_schema: &Schema) -> CqlQuery {
        use super::optimize::PathOptimizer;

        let optimizer = PathOptimizer::from_schema(source_schema);
        let mut optimized = self.clone();

        for block in &mut optimized.blocks {
            // Optimiser les WHERE
            for clause in &mut block.where_clauses {
                match clause {
                    WhereClause::Comparison { var, path, .. } => {
                        if path.len() >= 2 {
                            // Construire un Path et l'optimiser
                            // On utilise la variable FROM pour trouver l'entité de départ
                            if let Some(entity) = block.from_vars.get(var) {
                                let full_path = Path {
                                    start: entity.clone(),
                                    edges: path.clone(),
                                };
                                let opt = optimizer.optimize_path(&full_path);
                                *path = opt.edges;
                            }
                        }
                    }
                    WhereClause::PathEqual { var1, path1, var2, path2 } => {
                        if let Some(entity1) = block.from_vars.get(var1) {
                            let full1 = Path { start: entity1.clone(), edges: path1.clone() };
                            let opt1 = optimizer.optimize_path(&full1);
                            *path1 = opt1.edges;
                        }
                        if let Some(entity2) = block.from_vars.get(var2) {
                            let full2 = Path { start: entity2.clone(), edges: path2.clone() };
                            let opt2 = optimizer.optimize_path(&full2);
                            *path2 = opt2.edges;
                        }
                    }
                }
            }

            // Optimiser les bindings d'attributs
            for (_attr, binding) in &mut block.attribute_bindings {
                if binding.path.len() >= 2 {
                    if let Some(entity) = block.from_vars.get(&binding.from_var) {
                        let full_path = Path {
                            start: entity.clone(),
                            edges: binding.path.clone(),
                        };
                        let opt = optimizer.optimize_path(&full_path);
                        binding.path = opt.edges;
                    }
                }
            }
        }

        optimized
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::BaseType;

    #[test]
    fn test_create_query() {
        let mut query = CqlQuery::new("FindEngineers", "Company");

        let block = QueryBlock {
            target_entity: "Engineer".to_string(),
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
        };

        query.add_block(block);
        assert_eq!(query.blocks.len(), 1);
        assert!(query.result_schema.nodes.contains_key("Engineer"));
    }

    #[test]
    fn test_optimize_query() {
        // Schéma avec raccourci
        let mut schema = Schema::new("Company");
        schema.add_node("Employee")
              .add_node("Department")
              .add_fk("department", "Employee", "Department")
              .add_fk("manager", "Department", "Employee")
              .add_fk("direct_mgr", "Employee", "Employee")
              .add_attribute("emp_name", "Employee", BaseType::String)
              .add_attribute("dept_name", "Department", BaseType::String)
              .add_path_equation(
                  Path::new("Employee", vec!["department", "manager"]),
                  Path::new("Employee", vec!["direct_mgr"]),
              );

        let mut query = CqlQuery::new("FindManagedBy", "Company");
        let block = QueryBlock {
            target_entity: "Result".to_string(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    // Chemin long : e.department.manager.emp_name (3 JOINs)
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
            ]),
            fk_bindings: HashMap::new(),
        };
        query.add_block(block);

        let optimized = query.optimize(&schema);
        let opt_where = &optimized.blocks[0].where_clauses[0];

        if let WhereClause::Comparison { path, .. } = opt_where {
            // Devrait être optimisé : department.manager → direct_mgr
            // Donc le chemin passe de 3 éléments à 2 : [direct_mgr, emp_name]
            assert_eq!(path.len(), 2, "Le chemin devrait être raccourci à 2 arêtes");
            assert_eq!(path[0], "direct_mgr");
            assert_eq!(path[1], "emp_name");
            println!("Avant : [department, manager, emp_name] (3 JOINs)");
            println!("Après : {:?} ({} JOINs)", path, path.len());
        }
    }
}
