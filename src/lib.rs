// =============================================================================
// CATRUST — Moteur CQL (Categorical Query Language) en Rust
// =============================================================================
//
// Catrust implémente un moteur de requêtes et de migrations de données
// fondé sur la théorie des catégories, au-dessus de bases de données
// existantes (PostgreSQL, Snowflake, Neo4j...).
//
// Architecture :
//   core/     → Le cœur catégorique pur (aucune dépendance externe)
//   backend/  → Traduction vers les DB réelles (SQL, Cypher...)
//
// Concepts fondamentaux :
//   Schema   = une catégorie (nœuds + arêtes + équations de chemins)
//   Instance = un foncteur Schema → Set (les données)
//   Mapping  = un foncteur entre schémas (comment restructurer)
//   Δ, Σ, Π  = migrations catégoriques (pullback, pushforward, pi)
//
// =============================================================================

pub mod core;
pub mod backend;
