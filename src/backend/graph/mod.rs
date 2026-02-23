// =============================================================================
// BACKEND GRAPH — Génération de Cypher (Neo4j) à partir des catégories
// =============================================================================
//
// La traduction vers un graph DB est TRÈS naturelle car :
//   - Nœud CQL (entité) → Label Neo4j
//   - FK CQL → Relation Neo4j
//   - Attribut CQL → Propriété Neo4j
//   - Chemin CQL → Pattern Cypher
//
// Un Schema CQL *est* déjà un schéma de graphe !
//
// =============================================================================

use crate::core::schema::{Schema, Edge};
use crate::core::instance::Instance;
use crate::core::mapping::Mapping;
use crate::core::typeside::Value;
use crate::backend::{Backend, Statement};

/// Backend Neo4j — génère du Cypher
pub struct Neo4jBackend;

impl Neo4jBackend {
    pub fn new() -> Self {
        Neo4jBackend
    }

    /// Génère le Cypher pour créer un nœud avec ses propriétés
    fn create_node_cypher(
        entity_name: &str,
        row_id: u64,
        attrs: &std::collections::HashMap<String, Value>,
    ) -> String {
        let props: Vec<String> = std::iter::once(format!("catrust_id: {}", row_id))
            .chain(attrs.iter().map(|(k, v)| format!("{}: {}", k, value_to_cypher(v))))
            .collect();

        format!(
            "CREATE (:{} {{ {} }});",
            entity_name,
            props.join(", ")
        )
    }

    /// Génère le Cypher pour créer une relation entre deux nœuds
    fn create_relationship_cypher(
        source_entity: &str,
        source_id: u64,
        rel_name: &str,
        target_entity: &str,
        target_id: u64,
    ) -> String {
        format!(
            "MATCH (a:{} {{ catrust_id: {} }}), (b:{} {{ catrust_id: {} }}) CREATE (a)-[:{}]->(b);",
            source_entity, source_id,
            target_entity, target_id,
            rel_name.to_uppercase(),
        )
    }
}

/// Convertit une Value en littéral Cypher
fn value_to_cypher(value: &Value) -> String {
    match value {
        Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        Value::Integer(i) => format!("{}", i),
        Value::Float(f) => format!("{}", f),
        Value::Boolean(b) => if *b { "true".into() } else { "false".into() },
        Value::Null => "null".into(),
    }
}

impl Backend for Neo4jBackend {
    fn deploy_schema(&self, schema: &Schema) -> Vec<Statement> {
        let mut stmts = Vec::new();

        // Créer des contraintes d'unicité pour chaque label
        for entity_name in schema.nodes.keys() {
            stmts.push(Statement::Cypher(format!(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:{}) REQUIRE n.catrust_id IS UNIQUE;",
                entity_name
            )));
        }

        // Créer des index sur les propriétés fréquemment utilisées
        for edge in schema.edges.values() {
            if let Edge::Attribute { name, source, .. } = edge {
                stmts.push(Statement::Cypher(format!(
                    "CREATE INDEX IF NOT EXISTS FOR (n:{}) ON (n.{});",
                    source, name
                )));
            }
        }

        stmts
    }

    fn export_instance(&self, schema: &Schema, instance: &Instance) -> Vec<Statement> {
        let mut stmts = Vec::new();

        // Phase 1 : Créer tous les nœuds
        for (entity_name, entity_data) in &instance.data {
            for row_id in entity_data.row_ids() {
                let attrs = entity_data.attribute_values
                    .get(&row_id)
                    .cloned()
                    .unwrap_or_default();
                stmts.push(Statement::Cypher(
                    Neo4jBackend::create_node_cypher(entity_name, row_id, &attrs)
                ));
            }
        }

        // Phase 2 : Créer toutes les relations (FK)
        for (entity_name, entity_data) in &instance.data {
            for row_id in entity_data.row_ids() {
                if let Some(fks) = entity_data.fk_values.get(&row_id) {
                    for (fk_name, target_id) in fks {
                        // Trouver l'entité cible
                        if let Some(Edge::ForeignKey { target, .. }) = schema.edges.get(fk_name) {
                            stmts.push(Statement::Cypher(
                                Neo4jBackend::create_relationship_cypher(
                                    entity_name, row_id,
                                    fk_name, target, *target_id,
                                )
                            ));
                        }
                    }
                }
            }
        }

        stmts
    }

    fn generate_delta(&self, _mapping: &Mapping, _source: &Schema, _target: &Schema) -> Vec<Statement> {
        // TODO: Générer les MATCH ... RETURN pour Δ
        vec![Statement::Cypher("// TODO: Delta migration Cypher".into())]
    }

    fn generate_sigma(&self, _mapping: &Mapping, _source: &Schema, _target: &Schema) -> Vec<Statement> {
        // TODO: Générer les MERGE/CREATE pour Σ
        vec![Statement::Cypher("// TODO: Sigma migration Cypher".into())]
    }

    fn name(&self) -> &str {
        "Neo4j"
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
         .add_attribute("dept_name", "Department", BaseType::String);
        s
    }

    #[test]
    fn test_neo4j_schema() {
        let schema = company_schema();
        let backend = Neo4jBackend::new();
        let stmts = backend.deploy_schema(&schema);

        let cypher = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(cypher.contains("CREATE CONSTRAINT"));
        println!("=== Neo4j Schema ===\n{}", cypher);
    }

    #[test]
    fn test_neo4j_instance() {
        let schema = company_schema();
        let mut inst = Instance::new("Data", &schema);

        let d1 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
            HashMap::new(),
        );
        inst.insert("Employee",
            HashMap::from([("emp_name".into(), Value::String("Alice".into()))]),
            HashMap::from([("works_in".into(), d1)]),
        );

        let backend = Neo4jBackend::new();
        let stmts = backend.export_instance(&schema, &inst);

        let cypher = stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n");
        assert!(cypher.contains("CREATE"));
        assert!(cypher.contains("WORKS_IN"));
        println!("=== Neo4j Instance ===\n{}", cypher);
    }
}
