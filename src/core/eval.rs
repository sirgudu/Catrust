// =============================================================================
// EVAL — Évaluateur de requêtes CQL en mémoire
// =============================================================================
//
// Ce module évalue les CqlQuery DIRECTEMENT sur les Instance,
// sans passer par une base de données. Tout est en mémoire, en Rust pur.
//
// POURQUOI C'EST GÉNIAL :
//   1. Zéro dépendance (pas de PostgreSQL, Neo4j, rien)
//   2. Ultra-rapide (pas de réseau, pas de parsing SQL)
//   3. Parfait pour les puzzles (AoC), les tests, le prototypage
//   4. Mathématiquement, c'est l'évaluation directe du foncteur
//
// COMMENT ÇA MARCHE :
//
//   CQL Query :
//     from e : Employee
//     where e.department.dept_name = "Engineering"
//     where e.salary > 80000
//     return e.emp_name, e.department.dept_name
//
//   Algorithme :
//     1. Pour chaque variable FROM, itérer sur toutes les lignes de l'entité
//     2. Pour le produit cartésien des lignes (si plusieurs FROM)
//     3. Évaluer chaque WHERE : suivre les FK, lire l'attribut, comparer
//     4. Pour les lignes qui passent, projeter les bindings d'attributs
//     5. Construire l'Instance résultat
//
//   C'est exactement SELECT ... FROM ... WHERE en mémoire,
//   mais guidé par la structure catégorique (chemins = compositions de FK).
//
// OPTIMISATION :
//   Avant l'évaluation, on peut appliquer le PathOptimizer pour raccourcir
//   les chemins → moins de follow_path → plus rapide.
//
// =============================================================================

use std::collections::HashMap;
use super::schema::{Schema, Edge};
use super::instance::{Instance, RowId, EntityData};
use super::typeside::Value;
use super::query::{CqlQuery, QueryBlock, WhereClause, CompOp, AttributeBinding, FkBinding};

/// Résultat de l'évaluation d'une requête
#[derive(Debug, Clone)]
pub struct EvalResult {
    /// Les données résultat, par entité cible
    pub instance: Instance,
    /// Nombre de lignes évaluées (avant filtrage)
    pub rows_scanned: usize,
    /// Nombre de lignes retournées (après filtrage)
    pub rows_returned: usize,
    /// Temps d'évaluation (en microsecondes, si mesuré)
    pub eval_time_us: u128,
}

impl std::fmt::Display for EvalResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Résultat : {} lignes retournées ({} scannées, {}µs)",
                 self.rows_returned, self.rows_scanned, self.eval_time_us)?;
        // Afficher les données
        for (entity, data) in &self.instance.data {
            if data.is_empty() { continue; }
            writeln!(f, "  {} ({} lignes) :", entity, data.len())?;
            for row_id in data.row_ids() {
                write!(f, "    [{}]", row_id)?;
                if let Some(attrs) = data.attribute_values.get(&row_id) {
                    for (attr, val) in attrs {
                        write!(f, " {}={},", attr, val)?;
                    }
                }
                if let Some(fks) = data.fk_values.get(&row_id) {
                    for (fk, target) in fks {
                        write!(f, " {}→{},", fk, target)?;
                    }
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

/// Évalue une CqlQuery sur une Instance, en mémoire.
///
/// C'est LE point d'entrée principal pour l'exécution de requêtes.
///
/// # Arguments
/// * `query` - La requête CQL à évaluer
/// * `source` - L'instance source (les données)
/// * `schema` - Le schéma de l'instance source
///
/// # Retour
/// Un `EvalResult` contenant l'instance résultat et les métriques.
pub fn eval_query(
    query: &CqlQuery,
    source: &Instance,
    schema: &Schema,
) -> Result<EvalResult, String> {
    let start = std::time::Instant::now();
    let mut result_instance = Instance {
        name: format!("{}_result", query.name),
        schema_name: query.result_schema.name.clone(),
        data: HashMap::new(),
    };
    let mut total_scanned = 0usize;
    let mut total_returned = 0usize;

    for block in &query.blocks {
        let (entity_data, scanned, returned) = eval_block(block, source, schema)?;
        result_instance.data.insert(block.target_entity.clone(), entity_data);
        total_scanned += scanned;
        total_returned += returned;
    }

    let elapsed = start.elapsed().as_micros();

    Ok(EvalResult {
        instance: result_instance,
        rows_scanned: total_scanned,
        rows_returned: total_returned,
        eval_time_us: elapsed,
    })
}

/// Évalue un bloc de requête (un seul `entity ... { from ... where ... }`)
///
/// Stratégie :
///   1. Générer le produit cartésien de toutes les variables FROM
///   2. Pour chaque tuple, vérifier les clauses WHERE
///   3. Pour les tuples satisfaisants, projeter les attributs
fn eval_block(
    block: &QueryBlock,
    source: &Instance,
    schema: &Schema,
) -> Result<(EntityData, usize, usize), String> {
    let mut result = EntityData::new();
    let mut scanned = 0usize;

    // --- Étape 1 : collecter les RowId pour chaque variable FROM ---
    let var_names: Vec<&String> = block.from_vars.keys().collect();
    let var_entities: Vec<&String> = var_names.iter().map(|v| &block.from_vars[*v]).collect();

    // Collecter les RowId de chaque variable
    let var_rows: Vec<Vec<RowId>> = var_entities.iter().map(|entity| {
        source.data.get(*entity)
            .map(|ed| ed.row_ids())
            .unwrap_or_default()
    }).collect();

    // --- Étape 2 : produit cartésien des lignes ---
    // Pour un seul FROM (cas courant), c'est juste une itération simple.
    // Pour N FROM, c'est le produit cartésien.
    let tuples = cartesian_product(&var_rows);

    for tuple in &tuples {
        scanned += 1;

        // Construire le binding : var_name → (entity_name, row_id)
        let binding: HashMap<&str, (&str, RowId)> = var_names.iter()
            .enumerate()
            .map(|(i, &vn)| (vn.as_str(), (var_entities[i].as_str(), tuple[i])))
            .collect();

        // --- Étape 3 : vérifier les clauses WHERE ---
        let passes = eval_where_clauses(&block.where_clauses, &binding, source, schema)?;
        if !passes { continue; }

        // --- Étape 4 : projeter les attributs ---
        let mut attrs = HashMap::new();
        for (result_attr, ab) in &block.attribute_bindings {
            let val = eval_attribute_binding(ab, &binding, source, schema)?;
            attrs.insert(result_attr.clone(), val);
        }

        // --- Étape 5 : projeter les FK ---
        let mut fks = HashMap::new();
        for (result_fk, fb) in &block.fk_bindings {
            let target_row = eval_fk_binding(fb, &binding, source, schema)?;
            fks.insert(result_fk.clone(), target_row);
        }

        result.insert(attrs, fks);
    }

    let returned = result.len();
    Ok((result, scanned, returned))
}

/// Évalue toutes les clauses WHERE d'un binding. Retourne true si toutes passent.
fn eval_where_clauses(
    clauses: &[WhereClause],
    binding: &HashMap<&str, (&str, RowId)>,
    source: &Instance,
    schema: &Schema,
) -> Result<bool, String> {
    for clause in clauses {
        match clause {
            WhereClause::Comparison { var, path, op, value } => {
                let resolved = resolve_value(var, path, binding, source, schema)?;
                if !compare_values(&resolved, op, value) {
                    return Ok(false);
                }
            }
            WhereClause::PathEqual { var1, path1, var2, path2 } => {
                let v1 = resolve_value(var1, path1, binding, source, schema)?;
                let v2 = resolve_value(var2, path2, binding, source, schema)?;
                if v1 != v2 {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Résout un chemin (variable + arêtes) en une Value concrète.
///
/// Le chemin est de la forme : [fk1, fk2, ..., attribut]
/// On suit les FK puis on lit l'attribut final.
///
/// C'est l'évaluation du foncteur Instance sur un morphisme composé.
fn resolve_value(
    var: &str,
    path: &[String],
    binding: &HashMap<&str, (&str, RowId)>,
    source: &Instance,
    schema: &Schema,
) -> Result<Value, String> {
    let (start_entity, start_row) = binding.get(var)
        .ok_or_else(|| format!("Variable FROM '{}' non trouvée", var))?;

    if path.is_empty() {
        return Err(format!("Chemin vide pour la variable '{}'", var));
    }

    // Séparer : les FK (tout sauf le dernier) et l'attribut (le dernier)
    let last = &path[path.len() - 1];

    // Déterminer si le dernier est un attribut ou une FK
    let is_last_attr = matches!(
        schema.edges.get(last),
        Some(Edge::Attribute { .. })
    );

    if is_last_attr {
        // Suivre les FK intermédiaires
        let fk_path = &path[..path.len() - 1];
        let (current_entity, current_row) = follow_fks(
            start_entity, *start_row, fk_path, source, schema
        )?;

        // Lire l'attribut final
        source.data.get(&current_entity)
            .and_then(|ed| ed.get_attr(current_row, last))
            .cloned()
            .ok_or_else(|| format!(
                "Attribut '{}' non trouvé pour {}[{}]", last, current_entity, current_row
            ))
    } else {
        // Tout est FK — on résout le RowId final et on le retourne comme entier
        // (utile pour les comparaisons de FK : e1.department = e2.department)
        let (_entity, row) = follow_fks(start_entity, *start_row, path, source, schema)?;
        Ok(Value::Integer(row as i64))
    }
}

/// Suit une séquence de FK et retourne (entité_finale, row_id_final)
fn follow_fks(
    start_entity: &str,
    start_row: RowId,
    fk_path: &[String],
    source: &Instance,
    schema: &Schema,
) -> Result<(String, RowId), String> {
    let mut entity = start_entity.to_string();
    let mut row = start_row;

    for fk_name in fk_path {
        let edge = schema.edges.get(fk_name)
            .ok_or_else(|| format!("FK '{}' non trouvée dans le schéma", fk_name))?;

        match edge {
            Edge::ForeignKey { target, .. } => {
                row = source.data.get(&entity)
                    .and_then(|ed| ed.get_fk(row, fk_name))
                    .ok_or_else(|| format!(
                        "FK '{}' non définie pour {}[{}]", fk_name, entity, row
                    ))?;
                entity = target.clone();
            }
            _ => return Err(format!("'{}' n'est pas une FK", fk_name)),
        }
    }

    Ok((entity, row))
}

/// Évalue un AttributeBinding → Value
fn eval_attribute_binding(
    ab: &AttributeBinding,
    binding: &HashMap<&str, (&str, RowId)>,
    source: &Instance,
    schema: &Schema,
) -> Result<Value, String> {
    let mut full_path = ab.path.clone();
    full_path.push(ab.attribute.clone());
    resolve_value(&ab.from_var, &full_path, binding, source, schema)
}

/// Évalue un FkBinding → RowId cible
fn eval_fk_binding(
    fb: &FkBinding,
    binding: &HashMap<&str, (&str, RowId)>,
    source: &Instance,
    schema: &Schema,
) -> Result<RowId, String> {
    let (start_entity, start_row) = binding.get(fb.from_var.as_str())
        .ok_or_else(|| format!("Variable FROM '{}' non trouvée", fb.from_var))?;

    let (_entity, row) = follow_fks(start_entity, *start_row, &fb.path, source, schema)?;
    Ok(row)
}

/// Compare deux Values avec un opérateur.
fn compare_values(lhs: &Value, op: &CompOp, rhs: &Value) -> bool {
    match (lhs, rhs) {
        (Value::Integer(a), Value::Integer(b)) => match op {
            CompOp::Eq => a == b,
            CompOp::Neq => a != b,
            CompOp::Lt => a < b,
            CompOp::Gt => a > b,
            CompOp::Lte => a <= b,
            CompOp::Gte => a >= b,
        },
        (Value::Float(a), Value::Float(b)) => match op {
            CompOp::Eq => (a - b).abs() < f64::EPSILON,
            CompOp::Neq => (a - b).abs() >= f64::EPSILON,
            CompOp::Lt => a < b,
            CompOp::Gt => a > b,
            CompOp::Lte => a <= b,
            CompOp::Gte => a >= b,
        },
        (Value::String(a), Value::String(b)) => match op {
            CompOp::Eq => a == b,
            CompOp::Neq => a != b,
            CompOp::Lt => a < b,
            CompOp::Gt => a > b,
            CompOp::Lte => a <= b,
            CompOp::Gte => a >= b,
        },
        (Value::Boolean(a), Value::Boolean(b)) => match op {
            CompOp::Eq => a == b,
            CompOp::Neq => a != b,
            _ => false, // pas de < > pour les booléens
        },
        // Comparaisons croisées Int/Float
        (Value::Integer(a), Value::Float(b)) => {
            let af = *a as f64;
            match op {
                CompOp::Eq => (af - b).abs() < f64::EPSILON,
                CompOp::Neq => (af - b).abs() >= f64::EPSILON,
                CompOp::Lt => af < *b,
                CompOp::Gt => af > *b,
                CompOp::Lte => af <= *b,
                CompOp::Gte => af >= *b,
            }
        },
        (Value::Float(a), Value::Integer(b)) => {
            let bf = *b as f64;
            match op {
                CompOp::Eq => (a - bf).abs() < f64::EPSILON,
                CompOp::Neq => (a - bf).abs() >= f64::EPSILON,
                CompOp::Lt => *a < bf,
                CompOp::Gt => *a > bf,
                CompOp::Lte => *a <= bf,
                CompOp::Gte => *a >= bf,
            }
        },
        // NULL : rien n'est égal à NULL (sémantique SQL)
        (Value::Null, _) | (_, Value::Null) => match op {
            CompOp::Neq => true,
            _ => false,
        },
        // Types incompatibles → faux
        _ => false,
    }
}

/// Produit cartésien de N vecteurs de RowId.
///
/// Ex: [[1,2], [10,20]] → [[1,10], [1,20], [2,10], [2,20]]
fn cartesian_product(sets: &[Vec<RowId>]) -> Vec<Vec<RowId>> {
    if sets.is_empty() {
        return vec![vec![]];
    }
    if sets.len() == 1 {
        return sets[0].iter().map(|&r| vec![r]).collect();
    }

    let first = &sets[0];
    let rest = cartesian_product(&sets[1..]);

    let mut result = Vec::with_capacity(first.len() * rest.len());
    for &item in first {
        for r in &rest {
            let mut tuple = Vec::with_capacity(1 + r.len());
            tuple.push(item);
            tuple.extend(r);
            result.push(tuple);
        }
    }
    result
}

/// Version optimisée : évalue une requête après l'avoir optimisée
/// via le PathOptimizer du schéma.
pub fn eval_query_optimized(
    query: &CqlQuery,
    source: &Instance,
    schema: &Schema,
) -> Result<EvalResult, String> {
    let optimized = query.optimize(schema);
    eval_query(&optimized, source, schema)
}

// =============================================================================
// Fonctions utilitaires d'agrégation (pour AoC et au-delà)
// =============================================================================

/// Compte le nombre de lignes dans l'entité résultat.
///
/// Équivalent de COUNT(*) en SQL.
pub fn count(result: &EvalResult, entity: &str) -> usize {
    result.instance.data.get(entity)
        .map(|ed| ed.len())
        .unwrap_or(0)
}

/// Somme d'un attribut numérique dans le résultat.
///
/// Équivalent de SUM(attr) en SQL.
pub fn sum(result: &EvalResult, entity: &str, attr: &str) -> f64 {
    result.instance.data.get(entity)
        .map(|ed| {
            ed.row_ids().iter().filter_map(|&rid| {
                ed.get_attr(rid, attr).map(|v| match v {
                    Value::Integer(i) => *i as f64,
                    Value::Float(f) => *f,
                    _ => 0.0,
                })
            }).sum()
        })
        .unwrap_or(0.0)
}

/// Minimum d'un attribut numérique.
pub fn min_val(result: &EvalResult, entity: &str, attr: &str) -> Option<Value> {
    result.instance.data.get(entity).and_then(|ed| {
        ed.row_ids().iter().filter_map(|&rid| {
            ed.get_attr(rid, attr).cloned()
        }).min_by(|a, b| cmp_values(a, b))
    })
}

/// Maximum d'un attribut numérique.
pub fn max_val(result: &EvalResult, entity: &str, attr: &str) -> Option<Value> {
    result.instance.data.get(entity).and_then(|ed| {
        ed.row_ids().iter().filter_map(|&rid| {
            ed.get_attr(rid, attr).cloned()
        }).max_by(|a, b| cmp_values(a, b))
    })
}

/// Collecte les valeurs distinctes d'un attribut.
pub fn distinct(result: &EvalResult, entity: &str, attr: &str) -> Vec<Value> {
    let mut values: Vec<Value> = result.instance.data.get(entity)
        .map(|ed| {
            ed.row_ids().iter().filter_map(|&rid| {
                ed.get_attr(rid, attr).cloned()
            }).collect()
        })
        .unwrap_or_default();

    values.sort_by(cmp_values);
    values.dedup();
    values
}

/// Comparaison ordonnée de deux Value (pour trier)
fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::BaseType;
    use crate::core::schema::{Schema, Path};
    use crate::core::query::*;

    /// Schéma Company classique
    fn company_schema() -> Schema {
        let mut s = Schema::new("Company");
        s.add_node("Employee")
         .add_node("Department")
         .add_fk("works_in", "Employee", "Department")
         .add_attribute("emp_name", "Employee", BaseType::String)
         .add_attribute("salary", "Employee", BaseType::Integer)
         .add_attribute("dept_name", "Department", BaseType::String);
        s
    }

    /// Instance Company avec données
    fn company_instance(schema: &Schema) -> Instance {
        let mut inst = Instance::new("Données", schema);

        let d1 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
            HashMap::new(),
        );
        let d2 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Marketing".into()))]),
            HashMap::new(),
        );

        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Alice".into())),
                ("salary".into(), Value::Integer(90000)),
            ]),
            HashMap::from([("works_in".into(), d1)]),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Bob".into())),
                ("salary".into(), Value::Integer(75000)),
            ]),
            HashMap::from([("works_in".into(), d1)]),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Charlie".into())),
                ("salary".into(), Value::Integer(60000)),
            ]),
            HashMap::from([("works_in".into(), d2)]),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Diana".into())),
                ("salary".into(), Value::Integer(85000)),
            ]),
            HashMap::from([("works_in".into(), d2)]),
        );

        inst
    }

    #[test]
    fn test_eval_simple_select_all() {
        // SELECT * FROM Employee (pas de WHERE)
        let schema = company_schema();
        let inst = company_instance(&schema);

        let mut query = CqlQuery::new("AllEmployees", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "emp_name".into(),
                }),
                ("salary".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "salary".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let result = eval_query(&query, &inst, &schema).unwrap();
        println!("{}", result);
        assert_eq!(result.rows_returned, 4);
        assert_eq!(count(&result, "Result"), 4);
    }

    #[test]
    fn test_eval_filter_by_salary() {
        // SELECT emp_name FROM Employee WHERE salary > 80000
        let schema = company_schema();
        let inst = company_instance(&schema);

        let mut query = CqlQuery::new("HighEarners", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    path: vec!["salary".into()],
                    op: CompOp::Gt,
                    value: Value::Integer(80000),
                },
            ],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "emp_name".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let result = eval_query(&query, &inst, &schema).unwrap();
        println!("{}", result);
        // Alice (90000) et Diana (85000) passent
        assert_eq!(result.rows_returned, 2);
    }

    #[test]
    fn test_eval_filter_by_fk_attribute() {
        // SELECT emp_name FROM Employee WHERE department.dept_name = "Engineering"
        // C'est un JOIN en SQL ! Ici, on suit la FK en mémoire.
        let schema = company_schema();
        let inst = company_instance(&schema);

        let mut query = CqlQuery::new("Engineers", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    // e.works_in.dept_name (FK → attribut)
                    path: vec!["works_in".into(), "dept_name".into()],
                    op: CompOp::Eq,
                    value: Value::String("Engineering".into()),
                },
            ],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "emp_name".into(),
                }),
                ("dept".into(), AttributeBinding {
                    from_var: "e".into(),
                    path: vec!["works_in".into()],
                    attribute: "dept_name".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let result = eval_query(&query, &inst, &schema).unwrap();
        println!("{}", result);
        // Alice et Bob sont dans Engineering
        assert_eq!(result.rows_returned, 2);
    }

    #[test]
    fn test_eval_combined_filters() {
        // SELECT emp_name FROM Employee
        // WHERE works_in.dept_name = "Engineering" AND salary > 80000
        let schema = company_schema();
        let inst = company_instance(&schema);

        let mut query = CqlQuery::new("SeniorEngineers", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    path: vec!["works_in".into(), "dept_name".into()],
                    op: CompOp::Eq,
                    value: Value::String("Engineering".into()),
                },
                WhereClause::Comparison {
                    var: "e".into(),
                    path: vec!["salary".into()],
                    op: CompOp::Gt,
                    value: Value::Integer(80000),
                },
            ],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "emp_name".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let result = eval_query(&query, &inst, &schema).unwrap();
        println!("{}", result);
        // Seule Alice (Engineering, 90000) passe
        assert_eq!(result.rows_returned, 1);
    }

    #[test]
    fn test_aggregation_functions() {
        let schema = company_schema();
        let inst = company_instance(&schema);

        // SELECT salary FROM Employee
        let mut query = CqlQuery::new("Salaries", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![],
            attribute_bindings: HashMap::from([
                ("salary".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "salary".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let result = eval_query(&query, &inst, &schema).unwrap();

        // COUNT = 4
        assert_eq!(count(&result, "Result"), 4);

        // SUM = 90000 + 75000 + 60000 + 85000 = 310000
        assert_eq!(sum(&result, "Result", "salary"), 310000.0);

        // MIN = 60000
        assert_eq!(min_val(&result, "Result", "salary"), Some(Value::Integer(60000)));

        // MAX = 90000
        assert_eq!(max_val(&result, "Result", "salary"), Some(Value::Integer(90000)));

        println!("COUNT: {}", count(&result, "Result"));
        println!("SUM:   {}", sum(&result, "Result", "salary"));
        println!("MIN:   {:?}", min_val(&result, "Result", "salary"));
        println!("MAX:   {:?}", max_val(&result, "Result", "salary"));
    }

    #[test]
    fn test_eval_with_optimization() {
        // Schéma avec path equation
        let mut schema = Schema::new("OptimCompany");
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

        let mut inst = Instance::new("Data", &schema);
        let d1 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Eng".into()))]),
            HashMap::new(),
        );

        let mgr = inst.insert("Employee",
            HashMap::from([("emp_name".into(), Value::String("Alice".into()))]),
            HashMap::from([
                ("department".into(), d1),
                ("direct_mgr".into(), 1), // self-referencing pour simplifier
            ]),
        );

        // Affecter le manager au département
        inst.data.get_mut("Department").unwrap()
            .fk_values.get_mut(&d1).unwrap()
            .insert("manager".into(), mgr);

        let e2 = inst.insert("Employee",
            HashMap::from([("emp_name".into(), Value::String("Bob".into()))]),
            HashMap::from([
                ("department".into(), d1),
                ("direct_mgr".into(), mgr),
            ]),
        );

        // Requête avec chemin LONG : e.department.manager.emp_name
        // Sera optimisée en : e.direct_mgr.emp_name
        let mut query = CqlQuery::new("FindByMgr", "OptimCompany");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![
                WhereClause::Comparison {
                    var: "e".into(),
                    path: vec!["department".into(), "manager".into(), "emp_name".into()],
                    op: CompOp::Eq,
                    value: Value::String("Alice".into()),
                },
            ],
            attribute_bindings: HashMap::from([
                ("name".into(), AttributeBinding {
                    from_var: "e".into(), path: vec![], attribute: "emp_name".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        // Évaluation optimisée
        let result = eval_query_optimized(&query, &inst, &schema).unwrap();
        println!("{}", result);
        // Alice (dept_mgr=elle-même) et Bob (dept_mgr=Alice) devraient passer
        assert!(result.rows_returned >= 1, "Au moins Bob devrait passer");
    }

    #[test]
    fn test_distinct_values() {
        let schema = company_schema();
        let inst = company_instance(&schema);

        // SELECT dept_name FROM Employee (via FK)
        let mut query = CqlQuery::new("Depts", "Company");
        query.add_block(QueryBlock {
            target_entity: "Result".into(),
            from_vars: HashMap::from([("e".into(), "Employee".into())]),
            where_clauses: vec![],
            attribute_bindings: HashMap::from([
                ("dept".into(), AttributeBinding {
                    from_var: "e".into(),
                    path: vec!["works_in".into()],
                    attribute: "dept_name".into(),
                }),
            ]),
            fk_bindings: HashMap::new(),
        });

        let result = eval_query(&query, &inst, &schema).unwrap();
        let depts = distinct(&result, "Result", "dept");

        assert_eq!(depts.len(), 2); // Engineering et Marketing
        println!("Départements distincts : {:?}", depts);
    }

    #[test]
    fn test_cartesian_product() {
        let sets = vec![vec![1, 2], vec![10, 20]];
        let result = cartesian_product(&sets);
        assert_eq!(result.len(), 4);
        assert!(result.contains(&vec![1, 10]));
        assert!(result.contains(&vec![1, 20]));
        assert!(result.contains(&vec![2, 10]));
        assert!(result.contains(&vec![2, 20]));
    }
}
