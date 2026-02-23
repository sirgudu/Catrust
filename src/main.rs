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
}
