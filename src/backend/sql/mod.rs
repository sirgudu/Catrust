// =============================================================================
// BACKEND SQL — Génération de SQL à partir des structures catégoriques
// =============================================================================
//
// Ce module traduit :
//   Schema    → CREATE TABLE + FOREIGN KEY
//   Instance  → INSERT INTO
//   Mapping Δ → SELECT ... JOIN (vue / restructuration)
//   Mapping Σ → INSERT INTO ... SELECT ... UNION ALL
//
// Le trait SqlDialect permet de supporter les différences entre
// PostgreSQL, Snowflake, SQLite, etc.
//
// =============================================================================

pub mod planner;

use crate::core::schema::{Schema, Edge};
use crate::core::instance::Instance;
use crate::core::mapping::Mapping;
use crate::core::typeside::BaseType;
use crate::backend::{Backend, Statement};

/// Dialecte SQL — les différences entre les moteurs SQL.
/// Chaque moteur SQL a ses propres types et syntaxes.
pub trait SqlDialect {
    /// Traduit un BaseType en type SQL natif
    fn type_to_sql(&self, ty: &BaseType) -> String;
    
    /// Type pour les clés primaires auto-incrémentées
    fn auto_id_type(&self) -> String;
    
    /// Nom du dialecte
    fn dialect_name(&self) -> String;

    /// Quote un identifiant (table, colonne)
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }
}

// ─── PostgreSQL ──────────────────────────────────────────────────────────────

pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn type_to_sql(&self, ty: &BaseType) -> String {
        match ty {
            BaseType::String => "TEXT".to_string(),
            BaseType::Integer => "INTEGER".to_string(),
            BaseType::Float => "DOUBLE PRECISION".to_string(),
            BaseType::Boolean => "BOOLEAN".to_string(),
            BaseType::Custom(name) => name.clone(),
        }
    }

    fn auto_id_type(&self) -> String {
        "BIGSERIAL PRIMARY KEY".to_string()
    }

    fn dialect_name(&self) -> String {
        "PostgreSQL".to_string()
    }
}

// ─── Snowflake ───────────────────────────────────────────────────────────────

pub struct SnowflakeDialect;

impl SqlDialect for SnowflakeDialect {
    fn type_to_sql(&self, ty: &BaseType) -> String {
        match ty {
            BaseType::String => "VARCHAR".to_string(),
            BaseType::Integer => "NUMBER(38,0)".to_string(),
            BaseType::Float => "FLOAT".to_string(),
            BaseType::Boolean => "BOOLEAN".to_string(),
            BaseType::Custom(name) => name.clone(),
        }
    }

    fn auto_id_type(&self) -> String {
        "NUMBER(38,0) AUTOINCREMENT PRIMARY KEY".to_string()
    }

    fn dialect_name(&self) -> String {
        "Snowflake".to_string()
    }
}

// ─── Trino (ex-Presto) ──────────────────────────────────────────────────────
//
// Trino est un moteur de requêtes fédérées : il ne stocke pas de données
// lui-même mais requête des catalogues (Hive, Iceberg, Delta Lake, PostgreSQL...).
//
// Particularités :
//   - Pas de SERIAL/AUTOINCREMENT : on utilise BIGINT pour les ID
//   - Pas de FOREIGN KEY (pas de DDL contraignant) 
//   - CREATE TABLE AS SELECT (CTAS) est le pattern principal
//   - Quote les identifiants avec des guillemets doubles "
//   - Supporte les catalogues : catalog.schema.table
//

pub struct TrinoDialect {
    /// Catalogue Trino (ex: "hive", "iceberg", "postgresql")
    pub catalog: String,
    /// Schéma Trino dans le catalogue (ex: "default", "public")
    pub schema_name: String,
}

impl TrinoDialect {
    pub fn new(catalog: &str, schema_name: &str) -> Self {
        TrinoDialect {
            catalog: catalog.to_string(),
            schema_name: schema_name.to_string(),
        }
    }

    /// Retourne le nom complet catalog.schema.table
    pub fn full_table_name(&self, table: &str) -> String {
        format!("{}.{}.\"{}\"", self.catalog, self.schema_name, table)
    }
}

impl SqlDialect for TrinoDialect {
    fn type_to_sql(&self, ty: &BaseType) -> String {
        match ty {
            BaseType::String => "VARCHAR".to_string(),
            BaseType::Integer => "BIGINT".to_string(),
            BaseType::Float => "DOUBLE".to_string(),
            BaseType::Boolean => "BOOLEAN".to_string(),
            BaseType::Custom(name) => name.clone(),
        }
    }

    fn auto_id_type(&self) -> String {
        // Trino n'a pas d'auto-increment natif.
        // On utilise BIGINT et on gère les ID côté Catrust.
        "BIGINT".to_string()
    }

    fn dialect_name(&self) -> String {
        "Trino".to_string()
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name)
    }
}

// ─── Backend SQL générique ───────────────────────────────────────────────────

/// Backend SQL générique, paramétré par un dialecte.
pub struct SqlBackend<D: SqlDialect> {
    pub dialect: D,
}

impl<D: SqlDialect> SqlBackend<D> {
    pub fn new(dialect: D) -> Self {
        SqlBackend { dialect }
    }

    /// Génère le CREATE TABLE pour une entité donnée.
    fn create_table_sql(&self, entity_name: &str, schema: &Schema) -> String {
        let quoted = self.dialect.quote_identifier(entity_name);
        let mut columns = vec![
            format!("  catrust_id {}", self.dialect.auto_id_type()),
        ];

        // Attributs
        for edge in schema.attributes_of(entity_name) {
            if let Edge::Attribute { name, target, .. } = edge {
                columns.push(format!(
                    "  {} {}",
                    self.dialect.quote_identifier(name),
                    self.dialect.type_to_sql(target),
                ));
            }
        }

        // FK (colonnes de référence)
        for edge in schema.edges_from(entity_name) {
            if let Edge::ForeignKey { name, target, .. } = edge {
                columns.push(format!(
                    "  {} BIGINT REFERENCES {}(catrust_id)",
                    self.dialect.quote_identifier(name),
                    self.dialect.quote_identifier(target),
                ));
            }
        }

        format!("CREATE TABLE {} (\n{}\n);", quoted, columns.join(",\n"))
    }

    /// Génère les INSERT INTO pour les données d'une entité.
    fn insert_rows_sql(&self, entity_name: &str, _schema: &Schema, instance: &Instance) -> Vec<String> {
        let mut stmts = Vec::new();
        
        if let Some(entity_data) = instance.data.get(entity_name) {
            for row_id in entity_data.row_ids() {
                let mut col_names = vec!["catrust_id".to_string()];
                let mut col_values = vec![format!("{}", row_id)];

                // Attributs
                if let Some(attrs) = entity_data.attribute_values.get(&row_id) {
                    for (attr_name, value) in attrs {
                        col_names.push(self.dialect.quote_identifier(attr_name));
                        col_values.push(value_to_sql(value));
                    }
                }

                // FK
                if let Some(fks) = entity_data.fk_values.get(&row_id) {
                    for (fk_name, target_id) in fks {
                        col_names.push(self.dialect.quote_identifier(fk_name));
                        col_values.push(format!("{}", target_id));
                    }
                }

                stmts.push(format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    self.dialect.quote_identifier(entity_name),
                    col_names.join(", "),
                    col_values.join(", "),
                ));
            }
        }

        stmts
    }
}

/// Convertit une Value en littéral SQL
fn value_to_sql(value: &crate::core::typeside::Value) -> String {
    use crate::core::typeside::Value;
    match value {
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Integer(i) => format!("{}", i),
        Value::Float(f) => format!("{}", f),
        Value::Boolean(b) => if *b { "TRUE".into() } else { "FALSE".into() },
        Value::Null => "NULL".into(),
    }
}

impl<D: SqlDialect> Backend for SqlBackend<D> {
    fn deploy_schema(&self, schema: &Schema) -> Vec<Statement> {
        let mut stmts = Vec::new();

        // D'abord les tables sans FK (ordre topologique simplifié)
        // Phase 1 : CREATE TABLE (sans les REFERENCES pour éviter les dépendances circulaires)
        for entity_name in schema.nodes.keys() {
            stmts.push(Statement::Sql(self.create_table_sql(entity_name, schema)));
        }

        stmts
    }

    fn export_instance(&self, schema: &Schema, instance: &Instance) -> Vec<Statement> {
        let mut stmts = Vec::new();

        // D'abord les entités sans FK entrantes (pour respecter les REFERENCES)
        // Ordre simplifié : d'abord toutes les entités qui ne sont cible d'aucune FK
        // TODO: faire un vrai tri topologique
        for entity_name in schema.nodes.keys() {
            for sql in self.insert_rows_sql(entity_name, schema, instance) {
                stmts.push(Statement::Sql(sql));
            }
        }

        stmts
    }

    fn generate_delta(&self, _mapping: &Mapping, _source: &Schema, _target: &Schema) -> Vec<Statement> {
        // TODO: Générer les SELECT ... JOIN pour Δ
        vec![Statement::Sql("-- TODO: Delta migration SQL".into())]
    }

    fn generate_sigma(&self, _mapping: &Mapping, _source: &Schema, _target: &Schema) -> Vec<Statement> {
        // TODO: Générer les INSERT INTO ... SELECT pour Σ
        vec![Statement::Sql("-- TODO: Sigma migration SQL".into())]
    }

    fn name(&self) -> &str {
        "SQL"
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::{BaseType, Value};
    use crate::core::schema::Schema;
    use crate::core::instance::Instance;
    use std::collections::HashMap;

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

    #[test]
    fn test_postgres_ddl() {
        let schema = company_schema();
        let backend = SqlBackend::new(PostgresDialect);
        let stmts = backend.deploy_schema(&schema);

        assert!(!stmts.is_empty());
        let sql = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(sql.contains("CREATE TABLE"));
        println!("=== PostgreSQL DDL ===\n{}", sql);
    }

    #[test]
    fn test_snowflake_ddl() {
        let schema = company_schema();
        let backend = SqlBackend::new(SnowflakeDialect);
        let stmts = backend.deploy_schema(&schema);

        let sql = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(sql.contains("NUMBER(38,0)") || sql.contains("VARCHAR"));
        println!("=== Snowflake DDL ===\n{}", sql);
    }

    #[test]
    fn test_postgres_insert() {
        let schema = company_schema();
        let mut inst = Instance::new("Data", &schema);

        let d1 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
            HashMap::new(),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Alice".into())),
                ("salary".into(), Value::Integer(80000)),
            ]),
            HashMap::from([("works_in".into(), d1)]),
        );

        let backend = SqlBackend::new(PostgresDialect);
        let stmts = backend.export_instance(&schema, &inst);

        let sql = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(sql.contains("INSERT INTO"));
        println!("=== PostgreSQL DML ===\n{}", sql);
    }

    #[test]
    fn test_trino_ddl() {
        let schema = company_schema();
        let backend = SqlBackend::new(TrinoDialect::new("iceberg", "default"));
        let stmts = backend.deploy_schema(&schema);

        let sql = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(sql.contains("BIGINT"));
        assert!(sql.contains("VARCHAR"));
        // Trino n'a pas de SERIAL, donc pas de BIGSERIAL
        assert!(!sql.contains("SERIAL"));
        println!("=== Trino DDL ===\n{}", sql);
    }

    #[test]
    fn test_trino_insert() {
        let schema = company_schema();
        let mut inst = Instance::new("Data", &schema);

        let d1 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
            HashMap::new(),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Alice".into())),
                ("salary".into(), Value::Integer(80000)),
            ]),
            HashMap::from([("works_in".into(), d1)]),
        );

        let backend = SqlBackend::new(TrinoDialect::new("iceberg", "default"));
        let stmts = backend.export_instance(&schema, &inst);

        let sql = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(sql.contains("INSERT INTO"));
        println!("=== Trino DML ===\n{}", sql);
    }
}
