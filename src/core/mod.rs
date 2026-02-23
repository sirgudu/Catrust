// =============================================================================
// CORE — Module principal du cœur catégorique
// =============================================================================
//
// Ce module regroupe toute la logique mathématique pure :
// pas de SQL, pas de Neo4j, pas de réseau — uniquement des catégories,
// des foncteurs et des transformations naturelles.
//
// Architecture :
//   typeside  → les types primitifs (String, Int, Bool...)
//   schema    → la catégorie qui décrit la structure (= le "moule")
//   instance  → le foncteur Schema → Set (= les données concrètes)
//   mapping   → le foncteur entre schémas (= comment restructurer)
//   migrate   → les opérations Δ, Σ, Π (= les migrations catégoriques)
//   validate  → la vérification de cohérence
//   optimize  → réécriture de chemins (élimination de JOINs)
//   query     → requêtes CQL (composition Δ ∘ Σ)
//   eval      → évaluateur in-memory (zéro DB)
//
// =============================================================================

pub mod typeside;
pub mod schema;
pub mod optimize;
pub mod query;
pub mod eval;
pub mod instance;
pub mod mapping;
pub mod migrate;
pub mod validate;
