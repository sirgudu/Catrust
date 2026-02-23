// =============================================================================
// CATRUST — Point d'entrée : démonstration du moteur CQL
// =============================================================================
//
// Ce main.rs montre un exemple complet :
//   1. Définir deux schémas (ancien et nouveau format)
//   2. Créer un mapping entre eux (foncteur)
//   3. Migrer les données (Σ et Δ)
//   4. Générer le SQL (PostgreSQL + Snowflake) et le Cypher (Neo4j)
//
// =============================================================================

use std::collections::HashMap;
use catrust::core::typeside::BaseType;
use catrust::core::typeside::Value;
use catrust::core::schema::{Schema, Path};
use catrust::core::instance::Instance;
use catrust::core::mapping::Mapping;
use catrust::core::migrate;
use catrust::core::validate;
use catrust::backend::Backend;
use catrust::backend::sql::{SqlBackend, PostgresDialect, SnowflakeDialect, TrinoDialect};
use catrust::backend::graph::Neo4jBackend;

fn main() {
    println!("╔══════════════════════════════════════════════════╗");
    println!("║      CATRUST — Categorical Query Language        ║");
    println!("║      Moteur de migrations catégoriques           ║");
    println!("╚══════════════════════════════════════════════════╝\n");

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 1 : Définir le schéma source (l'ancien format)
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 1 : Schéma source (ancien format) ═══\n");

    let mut schema_old = Schema::new("OldCompany");
    schema_old
        .add_node("Person")
        .add_node("Dept")
        .add_fk("works_in", "Person", "Dept")
        .add_attribute("person_name", "Person", BaseType::String)
        .add_attribute("age", "Person", BaseType::Integer)
        .add_attribute("dept_name", "Dept", BaseType::String)
        .add_attribute("budget", "Dept", BaseType::Float);

    println!("{}\n", schema_old);

    // Valider le schéma
    match validate::validate_schema(&schema_old) {
        Ok(()) => println!("✓ Schéma source valide\n"),
        Err(errors) => {
            for e in errors {
                println!("✗ {}", e);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 2 : Peupler avec des données (Instance = foncteur)
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 2 : Données (foncteur Schema → Set) ═══\n");

    let mut instance_old = Instance::new("OldData", &schema_old);

    let dept_eng = instance_old.insert("Dept",
        HashMap::from([
            ("dept_name".into(), Value::String("Engineering".into())),
            ("budget".into(), Value::Float(500000.0)),
        ]),
        HashMap::new(),
    );
    let dept_mkt = instance_old.insert("Dept",
        HashMap::from([
            ("dept_name".into(), Value::String("Marketing".into())),
            ("budget".into(), Value::Float(300000.0)),
        ]),
        HashMap::new(),
    );

    instance_old.insert("Person",
        HashMap::from([
            ("person_name".into(), Value::String("Alice".into())),
            ("age".into(), Value::Integer(30)),
        ]),
        HashMap::from([("works_in".into(), dept_eng)]),
    );
    instance_old.insert("Person",
        HashMap::from([
            ("person_name".into(), Value::String("Bob".into())),
            ("age".into(), Value::Integer(25)),
        ]),
        HashMap::from([("works_in".into(), dept_eng)]),
    );
    instance_old.insert("Person",
        HashMap::from([
            ("person_name".into(), Value::String("Charlie".into())),
            ("age".into(), Value::Integer(35)),
        ]),
        HashMap::from([("works_in".into(), dept_mkt)]),
    );

    println!("{}", instance_old.display(&schema_old));

    // Valider l'instance
    match validate::validate_instance(&instance_old, &schema_old) {
        Ok(()) => println!("✓ Instance valide (fonctorialité respectée)\n"),
        Err(errors) => {
            for e in errors {
                println!("✗ {}", e);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 3 : Définir le schéma cible (nouveau format)
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 3 : Schéma cible (nouveau format) ═══\n");

    let mut schema_new = Schema::new("NewCompany");
    schema_new
        .add_node("Employee")
        .add_node("Department")
        .add_fk("department", "Employee", "Department")
        .add_attribute("full_name", "Employee", BaseType::String)
        .add_attribute("employee_age", "Employee", BaseType::Integer)
        .add_attribute("dept_label", "Department", BaseType::String)
        .add_attribute("dept_budget", "Department", BaseType::Float);

    println!("{}\n", schema_new);

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 4 : Mapping (foncteur entre les deux schémas)
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 4 : Mapping F : OldCompany → NewCompany ═══\n");

    let mut mapping = Mapping::new("Migrate", "OldCompany", "NewCompany");
    mapping
        .map_node("Person", "Employee")
        .map_node("Dept", "Department")
        .map_fk("works_in", Path::new("Employee", vec!["department"]))
        .map_attr_direct("person_name", "full_name")
        .map_attr_direct("age", "employee_age")
        .map_attr_direct("dept_name", "dept_label")
        .map_attr_direct("budget", "dept_budget");

    println!("{}\n", mapping);

    // Valider le mapping
    match mapping.validate(&schema_old, &schema_new) {
        Ok(()) => println!("✓ Mapping valide (c'est bien un foncteur)\n"),
        Err(e) => println!("✗ Mapping invalide : {}\n", e),
    }

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 5 : Migration Σ (pushforward)
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 5 : Migration Σ (pousser les données) ═══\n");

    let instance_new = migrate::sigma(&mapping, &schema_old, &schema_new, &instance_old);
    println!("{}", instance_new.display(&schema_new));

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 6 : Migration Δ (pullback — dans l'autre sens)
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 6 : Migration Δ (tirer les données de New vers Old) ═══\n");

    let instance_roundtrip = migrate::delta(&mapping, &schema_old, &schema_new, &instance_new);
    println!("{}", instance_roundtrip.display(&schema_old));

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 7 : Générer le SQL et le Cypher
    // ═══════════════════════════════════════════════════════════
    println!("═══ ÉTAPE 7 : Génération multi-backend ═══\n");

    // PostgreSQL
    println!("--- PostgreSQL DDL ---");
    let pg = SqlBackend::new(PostgresDialect);
    for stmt in pg.deploy_schema(&schema_new) {
        println!("{}", stmt);
    }

    println!("\n--- PostgreSQL DML ---");
    for stmt in pg.export_instance(&schema_new, &instance_new) {
        println!("{}", stmt);
    }

    // Snowflake
    println!("\n--- Snowflake DDL ---");
    let sf = SqlBackend::new(SnowflakeDialect);
    for stmt in sf.deploy_schema(&schema_new) {
        println!("{}", stmt);
    }

    // Trino (ex-Presto) — moteur fédéré
    println!("\n--- Trino DDL (catalogue Iceberg) ---");
    let trino = SqlBackend::new(TrinoDialect::new("iceberg", "default"));
    for stmt in trino.deploy_schema(&schema_new) {
        println!("{}", stmt);
    }

    println!("\n--- Trino DML ---");
    for stmt in trino.export_instance(&schema_new, &instance_new) {
        println!("{}", stmt);
    }

    // Neo4j
    println!("\n--- Neo4j (Cypher) Schema ---");
    let neo = Neo4jBackend::new();
    for stmt in neo.deploy_schema(&schema_new) {
        println!("{}", stmt);
    }

    println!("\n--- Neo4j (Cypher) Data ---");
    for stmt in neo.export_instance(&schema_new, &instance_new) {
        println!("{}", stmt);
    }

    println!("\n═══════════════════════════════════════════════════");
    println!("Migration catégorique complète !");
    println!("  {} entités source → {} entités cible", schema_old.nodes.len(), schema_new.nodes.len());
    println!("  {} lignes migrées via Σ", instance_new.total_rows());
    println!("  4 backends supportés : PostgreSQL, Snowflake, Trino, Neo4j");
    println!("═══════════════════════════════════════════════════");

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 8 : Optimisation catégorique des chemins (JOINs)
    // ═══════════════════════════════════════════════════════════
    println!("\n═══ ÉTAPE 8 : Optimisation catégorique (JOIN elimination) ═══\n");

    // On crée un schéma avec une path equation exploitable
    let mut schema_optim = Schema::new("CompanyOptim");
    schema_optim
        .add_node("Employee")
        .add_node("Department")
        .add_fk("department", "Employee", "Department")
        .add_fk("manager", "Department", "Employee")
        .add_fk("direct_mgr", "Employee", "Employee")
        .add_attribute("emp_name", "Employee", BaseType::String)
        .add_attribute("salary", "Employee", BaseType::Integer)
        .add_attribute("dept_name", "Department", BaseType::String)
        // ÉQUATION CATÉGORIQUE : employee.department.manager = employee.direct_mgr
        // Cela signifie : "le manager du département = le manager direct"
        .add_path_equation(
            Path::new("Employee", vec!["department", "manager"]),
            Path::new("Employee", vec!["direct_mgr"]),
        );

    println!("{}\n", schema_optim);

    // Démontrer l'optimisation de chemins
    use catrust::core::optimize::PathOptimizer;

    let optimizer = PathOptimizer::from_schema(&schema_optim);

    let paths_to_optimize = vec![
        Path::new("Employee", vec!["department", "manager"]),
        Path::new("Employee", vec!["department", "manager", "department"]),
        Path::new("Employee", vec!["department", "manager", "department", "manager"]),
        Path::new("Employee", vec!["direct_mgr"]),
    ];

    for path in &paths_to_optimize {
        let result = optimizer.optimize(path);
        if result.joins_eliminated > 0 {
            println!("  AVANT : {} ({} JOINs)", result.original, result.original.len());
            println!("  APRÈS : {} ({} JOINs)  → {} JOIN(s) éliminé(s) ✓",
                     result.optimized, result.optimized.len(), result.joins_eliminated);
            for rule in &result.rules_applied {
                println!("    Règle : {}", rule);
            }
        } else {
            println!("  {} → déjà optimal ({} JOIN)", path, path.len());
        }
        println!();
    }

    // Démontrer la génération SQL optimisée
    use catrust::core::query::*;
    use catrust::backend::sql::planner::SqlPlanner;

    println!("--- Requête CQL → SQL optimisé (PostgreSQL) ---\n");

    let planner = SqlPlanner::new(&PostgresDialect, &schema_optim);

    let mut query = CqlQuery::new("FindByManager", "CompanyOptim");
    query.add_block(QueryBlock {
        target_entity: "Result".into(),
        from_vars: HashMap::from([("e".into(), "Employee".into())]),
        where_clauses: vec![
            WhereClause::Comparison {
                var: "e".into(),
                // Chemin LONG : e.department.manager.emp_name (2 JOINs + attribut)
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
    for plan in &plans {
        println!("{}", plan);
    }

    // Analyse complète du schéma
    println!("--- Analyse d'optimisation du schéma ---\n");
    let analysis = optimizer.analyze_schema(&schema_optim);
    if analysis.is_empty() {
        println!("  Aucune optimisation trouvée (déjà optimal).");
    } else {
        println!("  {} optimisation(s) possibles :", analysis.len());
        for r in &analysis {
            println!("    {} → {} ({} JOIN(s) éliminé(s))", r.original, r.optimized, r.joins_eliminated);
        }
    }

    println!("\n═══════════════════════════════════════════════════");
    println!("Catrust — Moteur CQL catégorique complet");
    println!("  46 tests · 4 backends · optimiseur · évaluateur in-memory");
    println!("═══════════════════════════════════════════════════");

    // ═══════════════════════════════════════════════════════════
    // ÉTAPE 9 : Évaluation in-memory (zéro DB !)
    // ═══════════════════════════════════════════════════════════
    println!("\n═══ ÉTAPE 9 : Évaluateur in-memory (zéro DB) ═══\n");

    // Réutilisons le schéma Company simple
    use catrust::core::eval;

    let mut schema_eval = Schema::new("Company");
    schema_eval
        .add_node("Employee")
        .add_node("Department")
        .add_fk("works_in", "Employee", "Department")
        .add_attribute("emp_name", "Employee", BaseType::String)
        .add_attribute("salary", "Employee", BaseType::Integer)
        .add_attribute("dept_name", "Department", BaseType::String);

    let mut inst_eval = Instance::new("Données", &schema_eval);
    let d1 = inst_eval.insert("Department",
        HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
        HashMap::new(),
    );
    let d2 = inst_eval.insert("Department",
        HashMap::from([("dept_name".into(), Value::String("Marketing".into()))]),
        HashMap::new(),
    );
    inst_eval.insert("Employee",
        HashMap::from([
            ("emp_name".into(), Value::String("Alice".into())),
            ("salary".into(), Value::Integer(90000)),
        ]),
        HashMap::from([("works_in".into(), d1)]),
    );
    inst_eval.insert("Employee",
        HashMap::from([
            ("emp_name".into(), Value::String("Bob".into())),
            ("salary".into(), Value::Integer(75000)),
        ]),
        HashMap::from([("works_in".into(), d1)]),
    );
    inst_eval.insert("Employee",
        HashMap::from([
            ("emp_name".into(), Value::String("Charlie".into())),
            ("salary".into(), Value::Integer(60000)),
        ]),
        HashMap::from([("works_in".into(), d2)]),
    );
    inst_eval.insert("Employee",
        HashMap::from([
            ("emp_name".into(), Value::String("Diana".into())),
            ("salary".into(), Value::Integer(85000)),
        ]),
        HashMap::from([("works_in".into(), d2)]),
    );

    // Requête 1 : Ingénieurs gagnant + de 80k
    let mut q1 = CqlQuery::new("SeniorEngineers", "Company");
    q1.add_block(QueryBlock {
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
            ("salary".into(), AttributeBinding {
                from_var: "e".into(), path: vec![], attribute: "salary".into(),
            }),
            ("dept".into(), AttributeBinding {
                from_var: "e".into(), path: vec!["works_in".into()], attribute: "dept_name".into(),
            }),
        ]),
        fk_bindings: HashMap::new(),
    });

    println!("Requête : SELECT name, salary, dept FROM Employee");
    println!("          WHERE works_in.dept_name = 'Engineering' AND salary > 80000\n");

    let result = eval::eval_query(&q1, &inst_eval, &schema_eval).unwrap();
    println!("{}", result);

    // Agrégations
    println!("--- Agrégations in-memory ---");

    // Tous les salaires
    let mut q_all = CqlQuery::new("AllSalaries", "Company");
    q_all.add_block(QueryBlock {
        target_entity: "R".into(),
        from_vars: HashMap::from([("e".into(), "Employee".into())]),
        where_clauses: vec![],
        attribute_bindings: HashMap::from([
            ("salary".into(), AttributeBinding {
                from_var: "e".into(), path: vec![], attribute: "salary".into(),
            }),
            ("name".into(), AttributeBinding {
                from_var: "e".into(), path: vec![], attribute: "emp_name".into(),
            }),
            ("dept".into(), AttributeBinding {
                from_var: "e".into(), path: vec!["works_in".into()], attribute: "dept_name".into(),
            }),
        ]),
        fk_bindings: HashMap::new(),
    });

    let all = eval::eval_query(&q_all, &inst_eval, &schema_eval).unwrap();
    println!("  COUNT(*) = {}", eval::count(&all, "R"));
    println!("  SUM(salary) = {}", eval::sum(&all, "R", "salary"));
    println!("  MIN(salary) = {:?}", eval::min_val(&all, "R", "salary").unwrap());
    println!("  MAX(salary) = {:?}", eval::max_val(&all, "R", "salary").unwrap());
    println!("  DISTINCT(dept) = {:?}", eval::distinct(&all, "R", "dept"));
    println!("  Temps : {}µs", all.eval_time_us);
}
