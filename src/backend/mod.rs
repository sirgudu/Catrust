// =============================================================================
// BACKEND — Couche d'abstraction pour les bases de données
// =============================================================================
//
// Le backend traduit les concepts catégoriques en opérations concrètes
// sur une base de données. Grâce au trait Backend, on peut supporter :
//   - PostgreSQL (SQL)
//   - Snowflake (SQL avec dialecte spécifique)
//   - Neo4j (Cypher)
//   - DuckDB, SQLite, etc.
//
// Le cœur catégorique (module core) ne connaît JAMAIS les backends.
// C'est le backend qui traduit Schema → DDL, Instance → DML, etc.
//
// =============================================================================

pub mod sql;
pub mod graph;

use crate::core::schema::Schema;
use crate::core::instance::Instance;
use crate::core::mapping::Mapping;

/// Un statement généré par un backend.
/// Chaque backend produit ses propres commandes textuelles.
#[derive(Debug, Clone)]
pub enum Statement {
    /// Commande SQL (PostgreSQL, Snowflake, SQLite...)
    Sql(String),
    /// Commande Cypher (Neo4j)
    Cypher(String),
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Sql(s) => write!(f, "{}", s),
            Statement::Cypher(s) => write!(f, "{}", s),
        }
    }
}

/// Trait abstrait pour tous les backends de base de données.
///
/// Chaque backend implémente ce trait pour traduire les concepts
/// catégoriques en opérations concrètes sur la DB cible.
///
/// PHILOSOPHIE : le backend est un FONCTEUR du monde catégorique
/// vers le monde des commandes de base de données.
pub trait Backend {
    /// Génère les commandes DDL pour créer la structure du schéma.
    /// En SQL : CREATE TABLE, ALTER TABLE ADD FOREIGN KEY...
    /// En Cypher : CREATE CONSTRAINT, CREATE INDEX...
    fn deploy_schema(&self, schema: &Schema) -> Vec<Statement>;

    /// Génère les commandes DML pour insérer les données d'une instance.
    /// En SQL : INSERT INTO...
    /// En Cypher : CREATE (n:Label {props})...
    fn export_instance(&self, schema: &Schema, instance: &Instance) -> Vec<Statement>;

    /// Génère les commandes pour effectuer une migration Δ.
    /// En SQL : CREATE TABLE ... AS SELECT ... JOIN ...
    /// En Cypher : MATCH ... CREATE ...
    fn generate_delta(&self, mapping: &Mapping, source: &Schema, target: &Schema) -> Vec<Statement>;

    /// Génère les commandes pour effectuer une migration Σ.
    fn generate_sigma(&self, mapping: &Mapping, source: &Schema, target: &Schema) -> Vec<Statement>;

    /// Retourne le nom du backend
    fn name(&self) -> &str;
}
