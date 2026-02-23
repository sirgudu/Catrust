// =============================================================================
// MIGRATE — Les trois opérations fondamentales de migration : Δ, Σ, Π
// =============================================================================
//
// C'est ici que la magie catégorique opère. Étant donné un Mapping F : S → T
// (foncteur entre deux schémas), on peut migrer les données dans TROIS directions :
//
// ┌─────────────────────────────────────────────────────────────────────┐
// │                                                                     │
// │  Δ_F (Delta / Pullback)     — "Restructurer selon le source"        │
// │  Prend une instance de T, produit une instance de S                 │
// │  Direction : Instance(T) → Instance(S)                              │
// │  En SQL : des SELECT + JOIN pour reformater les données             │
// │                                                                     │
// │  Σ_F (Sigma / Left Kan Extension) — "Pousser vers la cible"        │
// │  Prend une instance de S, produit une instance de T                 │
// │  Direction : Instance(S) → Instance(T)                              │
// │  En SQL : des INSERT INTO ... SELECT avec identifications (UNION)   │
// │                                                                     │
// │  Π_F (Pi / Right Kan Extension) — "Jointure universelle"           │
// │  Prend une instance de S, produit une instance de T                 │
// │  Direction : Instance(S) → Instance(T)                              │
// │  En SQL : des requêtes avec produit cartésien filtré (JOIN complexe)│
// │                                                                     │
// └─────────────────────────────────────────────────────────────────────┘
//
// INTUITION :
//   Δ = "Vue" (on regarde les données de T à travers les lunettes de S)
//   Σ = "Export" (on envoie les données de S vers T, en fusionnant si nécessaire)
//   Π = "Requête universelle" (produit fibré, plus complexe)
//
// =============================================================================

use std::collections::HashMap;
use super::schema::{Schema, Edge};
use super::instance::{Instance, RowId};
use super::mapping::{Mapping, EdgeMapping};

/// Effectue la migration Delta : Δ_F(instance_T) → instance_S
///
/// Δ est un PULLBACK : on prend des données structurées selon T,
/// et on les réorganise selon S.
///
/// ALGORITHME :
/// Pour chaque nœud A dans S :
///   1. F(A) = B dans T → on récupère les lignes de B
///   2. Pour chaque arête a: A → C dans S :
///      - F(a) = chemin dans T → on suit ce chemin pour retrouver les données
///
/// C'est comme faire un SELECT ... JOIN ... en SQL : on ne crée pas de données,
/// on restructure les données existantes.
///
/// EXEMPLE :
///   F(Person) = Employee, F(Dept) = Department
///   Δ_F(instance de T) prend les Employee et les renomme en Person,
///   prend les Department et les renomme en Dept.
pub fn delta(
    mapping: &Mapping,
    source_schema: &Schema,
    target_schema: &Schema,
    target_instance: &Instance,
) -> Instance {
    let mut result = Instance::new(
        &format!("delta_{}", mapping.name),
        source_schema,
    );

    // Pour chaque nœud dans le schéma source S
    for (source_node, target_node) in &mapping.node_mapping {
        // F(source_node) = target_node → on copie les lignes de target_node
        if let Some(target_data) = target_instance.data.get(target_node) {
            let result_data = result.data.get_mut(source_node).unwrap();

            for row_id in target_data.row_ids() {
                let mut new_attrs = HashMap::new();
                let mut new_fks = HashMap::new();

                // Pour chaque arête sortant de ce nœud dans S
                for (edge_name, edge_mapping) in &mapping.edge_mapping {
                    // Vérifier que cette arête part bien de source_node
                    let source_edge = match source_schema.edges.get(edge_name) {
                        Some(e) if e.source() == source_node => e,
                        _ => continue,
                    };

                    match (source_edge, edge_mapping) {
                        // FK dans S → chemin de FK dans T
                        (Edge::ForeignKey { .. }, EdgeMapping::FkToPath(path)) => {
                            // Suivre le chemin dans T pour trouver le RowId cible
                            if let Some(target_row) = target_instance.follow_path(
                                target_node, row_id,
                                &path.edges,
                                target_schema,
                            ) {
                                new_fks.insert(edge_name.clone(), target_row);
                            }
                        }
                        // Attribut dans S → chemin + attribut dans T
                        (Edge::Attribute { .. }, EdgeMapping::AttrToPath { fk_path, attr_name }) => {
                            // Suivre le chemin de FK dans T, puis lire l'attribut
                            let resolved_row = if fk_path.is_empty() {
                                Some(row_id)
                            } else {
                                target_instance.follow_path(
                                    target_node, row_id,
                                    &fk_path.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                                    target_schema,
                                )
                            };

                            if let Some(resolved) = resolved_row {
                                // Trouver l'entité au bout du chemin FK
                                let mut entity = target_node.clone();
                                for fk in fk_path {
                                    if let Some(Edge::ForeignKey { target, .. }) = target_schema.edges.get(fk) {
                                        entity = target.clone();
                                    }
                                }
                                if let Some(target_data) = target_instance.data.get(&entity) {
                                    if let Some(value) = target_data.get_attr(resolved, attr_name) {
                                        new_attrs.insert(edge_name.clone(), value.clone());
                                    }
                                }
                            }
                        }
                        _ => {} // Mapping incohérent, on ignore
                    }
                }

                result_data.insert_with_id(row_id, new_attrs, new_fks);
            }
        }
    }

    result
}

/// Effectue la migration Sigma : Σ_F(instance_S) → instance_T
///
/// Σ est un PUSHFORWARD (extension de Kan à gauche) : on pousse les données
/// de S vers T, en identifiant (fusionnant) les lignes qui doivent l'être.
///
/// ALGORITHME :
/// Pour chaque nœud B dans T :
///   1. Collecter tous les nœuds A dans S tels que F(A) = B
///   2. Toutes les lignes de ces A deviennent des lignes de B
///   3. Si une FK dans S envoie deux lignes au même endroit dans T,
///      elles sont identifiées (fusionnées = union-find / quotient)
///
/// C'est comme faire des INSERT INTO ... SELECT ... UNION ALL en SQL.
///
/// EXEMPLE SIMPLE (sans identifications) :
///   F(Person) = User → les lignes de Person deviennent des lignes de User
///   F(person_name) = username → la colonne person_name devient username
pub fn sigma(
    mapping: &Mapping,
    source_schema: &Schema,
    target_schema: &Schema,
    source_instance: &Instance,
) -> Instance {
    let mut result = Instance::new(
        &format!("sigma_{}", mapping.name),
        target_schema,
    );

    // Construire le mapping inverse : pour chaque nœud de T, quels nœuds de S y sont envoyés ?
    let mut inverse_node_map: HashMap<String, Vec<String>> = HashMap::new();
    for (src, tgt) in &mapping.node_mapping {
        inverse_node_map
            .entry(tgt.clone())
            .or_default()
            .push(src.clone());
    }

    // Table de traduction des RowId : (entity_source, old_row_id) → new_row_id dans T
    let mut id_translation: HashMap<(String, RowId), RowId> = HashMap::new();

    // Phase 1 : Copier les lignes (avec nouveaux attributs)
    for (target_node, source_nodes) in &inverse_node_map {
        for source_node in source_nodes {
            if let Some(source_data) = source_instance.data.get(source_node) {
                for old_row_id in source_data.row_ids() {
                    let mut new_attrs = HashMap::new();

                    // Mapper les attributs
                    for (edge_name, edge_mapping) in &mapping.edge_mapping {
                        let source_edge = match source_schema.edges.get(edge_name) {
                            Some(e) if e.source() == source_node => e,
                            _ => continue,
                        };

                        if let (Edge::Attribute { .. }, EdgeMapping::AttrToPath { fk_path, attr_name }) =
                            (source_edge, edge_mapping)
                        {
                            if fk_path.is_empty() {
                                if let Some(value) = source_data.get_attr(old_row_id, edge_name) {
                                    new_attrs.insert(attr_name.clone(), value.clone());
                                }
                            }
                            // TODO: gérer les fk_path non-vides (nécessite résolution)
                        }
                    }

                    let new_row_id = result.data
                        .get_mut(target_node)
                        .unwrap()
                        .insert(new_attrs, HashMap::new());

                    id_translation.insert(
                        (source_node.clone(), old_row_id),
                        new_row_id,
                    );
                }
            }
        }
    }

    // Phase 2 : Résoudre les FK
    for (source_node, target_node) in &mapping.node_mapping {
        if let Some(source_data) = source_instance.data.get(source_node) {
            for old_row_id in source_data.row_ids() {
                let new_row_id = id_translation[&(source_node.clone(), old_row_id)];
                let mut new_fks = HashMap::new();

                for (edge_name, edge_mapping) in &mapping.edge_mapping {
                    let source_edge = match source_schema.edges.get(edge_name) {
                        Some(e) if e.source() == source_node => e,
                        _ => continue,
                    };

                    if let (
                        Edge::ForeignKey { target: fk_target, .. },
                        EdgeMapping::FkToPath(path),
                    ) = (source_edge, edge_mapping)
                    {
                        if let Some(old_target_row) = source_data.get_fk(old_row_id, edge_name) {
                            // Traduire le RowId cible dans le nouveau système
                            if let Some(&new_target_row) = id_translation.get(&(fk_target.clone(), old_target_row)) {
                                // Le chemin image : on prend la première FK du chemin
                                // (cas simple : chemin de longueur 1)
                                if path.edges.len() == 1 {
                                    new_fks.insert(path.edges[0].clone(), new_target_row);
                                }
                                // TODO: gérer les chemins de longueur > 1
                            }
                        }
                    }
                }

                // Mettre à jour les FK de la ligne
                if !new_fks.is_empty() {
                    if let Some(target_data) = result.data.get_mut(target_node) {
                        if let Some(existing_fks) = target_data.fk_values.get_mut(&new_row_id) {
                            existing_fks.extend(new_fks);
                        }
                    }
                }
            }
        }
    }

    result
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::{BaseType, Value};
    use crate::core::schema::{Schema, Path};

    /// Schéma source : l'ancien format
    fn old_schema() -> Schema {
        let mut s = Schema::new("Old");
        s.add_node("Person")
         .add_node("Dept")
         .add_fk("works_in", "Person", "Dept")
         .add_attribute("person_name", "Person", BaseType::String)
         .add_attribute("dept_name", "Dept", BaseType::String);
        s
    }

    /// Schéma cible : le nouveau format (renommage)
    fn new_schema() -> Schema {
        let mut s = Schema::new("New");
        s.add_node("Employee")
         .add_node("Department")
         .add_fk("department", "Employee", "Department")
         .add_attribute("emp_name", "Employee", BaseType::String)
         .add_attribute("dept_label", "Department", BaseType::String);
        s
    }

    /// Mapping simple : renommage des tables et colonnes
    fn rename_mapping() -> Mapping {
        let mut m = Mapping::new("Rename", "Old", "New");
        m.map_node("Person", "Employee")
         .map_node("Dept", "Department")
         .map_fk("works_in", Path::new("Employee", vec!["department"]))
         .map_attr_direct("person_name", "emp_name")
         .map_attr_direct("dept_name", "dept_label");
        m
    }

    /// Données dans l'ancien format
    fn old_instance(schema: &Schema) -> Instance {
        let mut inst = Instance::new("OldData", schema);

        let d1 = inst.insert("Dept",
            HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
            HashMap::new(),
        );
        let d2 = inst.insert("Dept",
            HashMap::from([("dept_name".into(), Value::String("Marketing".into()))]),
            HashMap::new(),
        );
        inst.insert("Person",
            HashMap::from([("person_name".into(), Value::String("Alice".into()))]),
            HashMap::from([("works_in".into(), d1)]),
        );
        inst.insert("Person",
            HashMap::from([("person_name".into(), Value::String("Bob".into()))]),
            HashMap::from([("works_in".into(), d2)]),
        );

        inst
    }

    #[test]
    fn test_sigma_simple_rename() {
        let s_old = old_schema();
        let s_new = new_schema();
        let m = rename_mapping();
        let inst_old = old_instance(&s_old);

        // Σ : pousser les données de Old vers New
        let inst_new = sigma(&m, &s_old, &s_new, &inst_old);

        // On doit avoir 2 Employee et 2 Department
        assert_eq!(inst_new.data["Employee"].len(), 2);
        assert_eq!(inst_new.data["Department"].len(), 2);

        // Vérifier qu'Alice existe dans Employee avec le bon nom d'attribut
        let emp_rows = inst_new.data["Employee"].row_ids();
        let has_alice = emp_rows.iter().any(|&id| {
            inst_new.data["Employee"]
                .get_attr(id, "emp_name")
                .map_or(false, |v| *v == Value::String("Alice".into()))
        });
        assert!(has_alice, "Alice devrait exister dans Employee");
    }

    #[test]
    fn test_delta_simple_rename() {
        let s_old = old_schema();
        let s_new = new_schema();
        let m = rename_mapping();

        // Créer des données dans le nouveau format
        let mut inst_new = Instance::new("NewData", &s_new);
        let d1 = inst_new.insert("Department",
            HashMap::from([("dept_label".into(), Value::String("R&D".into()))]),
            HashMap::new(),
        );
        inst_new.insert("Employee",
            HashMap::from([("emp_name".into(), Value::String("Diana".into()))]),
            HashMap::from([("department".into(), d1)]),
        );

        // Δ : tirer les données de New vers Old
        let inst_old = delta(&m, &s_old, &s_new, &inst_new);

        assert_eq!(inst_old.data["Person"].len(), 1);
        assert_eq!(inst_old.data["Dept"].len(), 1);

        // Vérifier que Diana est renommée en person_name
        let person_rows = inst_old.data["Person"].row_ids();
        let has_diana = person_rows.iter().any(|&id| {
            inst_old.data["Person"]
                .get_attr(id, "person_name")
                .map_or(false, |v| *v == Value::String("Diana".into()))
        });
        assert!(has_diana, "Diana devrait exister dans Person");
    }
}
