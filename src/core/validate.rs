// =============================================================================
// VALIDATE — Vérification des contraintes catégoriques
// =============================================================================
//
// Ce module vérifie que les structures sont cohérentes :
//   - Un Schema est bien formé (pas d'arêtes orphelines)
//   - Une Instance respecte les équations de chemins du Schema
//   - Un Mapping est un foncteur valide
//
// C'est crucial car la théorie des catégories donne des GARANTIES
// de correction des migrations SEULEMENT si les structures sont valides.
//
// =============================================================================

use super::schema::{Schema, Edge};
use super::instance::Instance;

/// Erreur de validation
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Validation error: {}", self.message)
    }
}

/// Vérifie qu'un Schema est bien formé.
///
/// Conditions :
/// - Toute arête FK référence des nœuds qui existent
/// - Tout attribut référence un nœud qui existe
/// - Les équations de chemins sont sur des nœuds/arêtes qui existent
pub fn validate_schema(schema: &Schema) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    for (edge_name, edge) in &schema.edges {
        match edge {
            Edge::ForeignKey { source, target, .. } => {
                if !schema.nodes.contains_key(source) {
                    errors.push(ValidationError {
                        message: format!(
                            "FK '{}' : le nœud source '{}' n'existe pas",
                            edge_name, source
                        ),
                    });
                }
                if !schema.nodes.contains_key(target) {
                    errors.push(ValidationError {
                        message: format!(
                            "FK '{}' : le nœud cible '{}' n'existe pas",
                            edge_name, target
                        ),
                    });
                }
            }
            Edge::Attribute { source, .. } => {
                if !schema.nodes.contains_key(source) {
                    errors.push(ValidationError {
                        message: format!(
                            "Attribut '{}' : le nœud source '{}' n'existe pas",
                            edge_name, source
                        ),
                    });
                }
            }
        }
    }

    // Vérifier les équations de chemins
    for (i, eq) in schema.path_equations.iter().enumerate() {
        // Vérifier que le nœud de départ de chaque côté existe
        if !schema.nodes.contains_key(&eq.lhs.start) {
            errors.push(ValidationError {
                message: format!(
                    "Équation {} : le nœud de départ '{}' (lhs) n'existe pas",
                    i, eq.lhs.start
                ),
            });
        }
        if !schema.nodes.contains_key(&eq.rhs.start) {
            errors.push(ValidationError {
                message: format!(
                    "Équation {} : le nœud de départ '{}' (rhs) n'existe pas",
                    i, eq.rhs.start
                ),
            });
        }

        // Vérifier que chaque arête du chemin existe
        for edge_name in &eq.lhs.edges {
            if !schema.edges.contains_key(edge_name) {
                errors.push(ValidationError {
                    message: format!(
                        "Équation {} : l'arête '{}' (lhs) n'existe pas",
                        i, edge_name
                    ),
                });
            }
        }
        for edge_name in &eq.rhs.edges {
            if !schema.edges.contains_key(edge_name) {
                errors.push(ValidationError {
                    message: format!(
                        "Équation {} : l'arête '{}' (rhs) n'existe pas",
                        i, edge_name
                    ),
                });
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Vérifie qu'une Instance respecte le Schema.
///
/// Conditions :
/// - Chaque FK est une fonction totale (chaque ligne a une FK bien définie)
/// - Les FK pointent vers des lignes qui existent
/// - Les équations de chemins sont satisfaites pour toutes les lignes
pub fn validate_instance(instance: &Instance, schema: &Schema) -> Result<(), Vec<ValidationError>> {
    let mut errors = Vec::new();

    for (entity_name, entity_data) in &instance.data {
        // Vérifier que l'entité existe dans le schéma
        if !schema.nodes.contains_key(entity_name) {
            errors.push(ValidationError {
                message: format!("L'entité '{}' n'existe pas dans le schéma", entity_name),
            });
            continue;
        }

        // Vérifier les FK sortantes
        let fks: Vec<_> = schema.edges.values()
            .filter(|e| {
                if let Edge::ForeignKey { source, .. } = e {
                    source == entity_name
                } else {
                    false
                }
            })
            .collect();

        for row_id in entity_data.row_ids() {
            for fk in &fks {
                if let Edge::ForeignKey { name, target, .. } = fk {
                    match entity_data.get_fk(row_id, name) {
                        None => {
                            errors.push(ValidationError {
                                message: format!(
                                    "{} row[{}] : FK '{}' manquante",
                                    entity_name, row_id, name
                                ),
                            });
                        }
                        Some(target_row_id) => {
                            // Vérifier que la ligne cible existe
                            if let Some(target_data) = instance.data.get(target) {
                                if !target_data.attribute_values.contains_key(&target_row_id) {
                                    errors.push(ValidationError {
                                        message: format!(
                                            "{} row[{}] : FK '{}' pointe vers {}[{}] qui n'existe pas",
                                            entity_name, row_id, name, target, target_row_id
                                        ),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Vérifier les équations de chemins
    for eq in &schema.path_equations {
        if let Some(entity_data) = instance.data.get(&eq.lhs.start) {
            for row_id in entity_data.row_ids() {
                let lhs_result = instance.follow_path(
                    &eq.lhs.start, row_id,
                    &eq.lhs.edges, schema,
                );
                let rhs_result = instance.follow_path(
                    &eq.rhs.start, row_id,
                    &eq.rhs.edges, schema,
                );

                if lhs_result != rhs_result {
                    errors.push(ValidationError {
                        message: format!(
                            "Équation de chemins violée pour row[{}] : {} ≠ {} ({:?} vs {:?})",
                            row_id, eq.lhs, eq.rhs, lhs_result, rhs_result
                        ),
                    });
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
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
    use std::collections::HashMap;

    #[test]
    fn test_validate_schema_ok() {
        let mut s = Schema::new("Test");
        s.add_node("A").add_node("B").add_fk("f", "A", "B");
        assert!(validate_schema(&s).is_ok());
    }

    #[test]
    fn test_validate_instance_ok() {
        let mut s = Schema::new("Test");
        s.add_node("A")
         .add_node("B")
         .add_fk("f", "A", "B")
         .add_attribute("name", "A", BaseType::String);

        let mut inst = Instance::new("TestData", &s);
        let b1 = inst.insert("B", HashMap::new(), HashMap::new());
        inst.insert("A",
            HashMap::from([("name".into(), Value::String("test".into()))]),
            HashMap::from([("f".into(), b1)]),
        );

        assert!(validate_instance(&inst, &s).is_ok());
    }

    #[test]
    fn test_validate_instance_broken_fk() {
        let mut s = Schema::new("Test");
        s.add_node("A").add_node("B").add_fk("f", "A", "B");

        let mut inst = Instance::new("TestData", &s);
        // On insère un A qui pointe vers un B inexistant (row_id 999)
        inst.insert("A",
            HashMap::new(),
            HashMap::from([("f".into(), 999)]),
        );

        let result = validate_instance(&inst, &s);
        assert!(result.is_err());
    }
}
