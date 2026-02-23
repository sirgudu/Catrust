// =============================================================================
// MAPPING — Un foncteur entre deux Schemas (catégories)
// =============================================================================
//
// En CQL, un Mapping F : S → T est un FONCTEUR entre deux catégories-schémas.
//
// Concrètement, F assigne :
//   - À chaque NŒUD de S, un NŒUD de T
//     (chaque table source correspond à une table cible)
//   - À chaque ARÊTE de S, un CHEMIN de T  
//     (chaque FK/attribut source correspond à une séquence de FK/attributs cibles)
//
// ET F doit respecter la composition :
//   Si en S on a le chemin A --f--> B --g--> C
//   alors en T on a F(g) ∘ F(f) = F(g∘f)
//   c'est-à-dire : les chemins images se composent correctement.
//
// POURQUOI C'EST PUISSANT :
//   Un Mapping permet de décrire n'importe quelle restructuration de schéma :
//   - Renommer des tables/colonnes
//   - Fusionner deux tables en une
//   - Splitter une table en deux
//   - Déplacer une colonne d'une table à une autre
//   - Aplatir des relations
//
// EXEMPLE :
//   Schema S (ancienne structure) :
//     Person --friend_of--> Person
//     Person --name--> String
//
//   Schema T (nouvelle structure) :
//     User --buddy--> User
//     User --username--> String
//
//   Mapping F : S → T :
//     F(Person) = User
//     F(friend_of) = buddy
//     F(name) = username
//
// Les MIGRATIONS DE DONNÉES (Δ, Σ, Π) utilisent ces Mappings.
//
// =============================================================================

use std::collections::HashMap;
use super::schema::{Schema, Path, Edge};

/// Correspondance pour une arête : vers quel chemin dans le schéma cible
/// cette arête est-elle envoyée ?
///
/// - Pour une FK : on mappe vers un chemin de FK dans T
/// - Pour un attribut : on mappe vers un chemin se terminant par un attribut dans T
#[derive(Debug, Clone)]
pub enum EdgeMapping {
    /// FK mappée vers un chemin de FK dans le schéma cible
    FkToPath(Path),
    /// Attribut mappé vers un chemin (FK*) puis attribut dans le schéma cible
    AttrToPath {
        /// Chemin de FK à suivre dans T avant d'atteindre l'attribut
        fk_path: Vec<String>,
        /// Nom de l'attribut final dans T
        attr_name: String,
    },
}

/// Un Mapping F : source_schema → target_schema.
///
/// C'est un foncteur entre catégories, qui associe :
/// - nœuds de S → nœuds de T
/// - arêtes de S → chemins de T
#[derive(Debug, Clone)]
pub struct Mapping {
    /// Nom de ce mapping
    pub name: String,
    /// Nom du schéma source
    pub source_schema_name: String,
    /// Nom du schéma cible
    pub target_schema_name: String,
    /// Correspondance des nœuds : node_S → node_T
    pub node_mapping: HashMap<String, String>,
    /// Correspondance des arêtes : edge_name_S → EdgeMapping dans T
    pub edge_mapping: HashMap<String, EdgeMapping>,
}

impl Mapping {
    /// Crée un nouveau Mapping vide entre deux schémas
    pub fn new(name: &str, source: &str, target: &str) -> Self {
        Mapping {
            name: name.to_string(),
            source_schema_name: source.to_string(),
            target_schema_name: target.to_string(),
            node_mapping: HashMap::new(),
            edge_mapping: HashMap::new(),
        }
    }

    /// Mappe un nœud source vers un nœud cible.
    /// F(node_source) = node_target
    pub fn map_node(&mut self, source: &str, target: &str) -> &mut Self {
        self.node_mapping.insert(source.to_string(), target.to_string());
        self
    }

    /// Mappe une FK source vers un chemin de FK cibles.
    /// F(fk_source) = chemin [fk1, fk2, ...] dans T
    /// 
    /// Si le chemin est vide, c'est l'identité (la FK est "oubliée" car
    /// source et cible sont mappées au même nœud).
    pub fn map_fk(&mut self, source_fk: &str, target_path: Path) -> &mut Self {
        self.edge_mapping.insert(
            source_fk.to_string(),
            EdgeMapping::FkToPath(target_path),
        );
        self
    }

    /// Mappe un attribut source vers un chemin + attribut dans T.
    /// F(attr_source) = fk_path ∘ attr_target
    pub fn map_attr(
        &mut self,
        source_attr: &str,
        fk_path: Vec<&str>,
        target_attr: &str,
    ) -> &mut Self {
        self.edge_mapping.insert(
            source_attr.to_string(),
            EdgeMapping::AttrToPath {
                fk_path: fk_path.into_iter().map(|s| s.to_string()).collect(),
                attr_name: target_attr.to_string(),
            },
        );
        self
    }

    /// Mappe un attribut source directement vers un attribut cible (cas simple, sans chemin FK).
    /// F(attr_source) = attr_target (dans le même nœud image)
    pub fn map_attr_direct(
        &mut self,
        source_attr: &str,
        target_attr: &str,
    ) -> &mut Self {
        self.edge_mapping.insert(
            source_attr.to_string(),
            EdgeMapping::AttrToPath {
                fk_path: vec![],
                attr_name: target_attr.to_string(),
            },
        );
        self
    }

    /// Vérifie que le mapping est complet (chaque nœud et arête de S est mappé).
    pub fn is_complete(&self, source_schema: &Schema) -> bool {
        // Chaque nœud de S doit être mappé
        for node_name in source_schema.nodes.keys() {
            if !self.node_mapping.contains_key(node_name) {
                return false;
            }
        }
        // Chaque arête de S doit être mappée
        for edge_name in source_schema.edges.keys() {
            if !self.edge_mapping.contains_key(edge_name) {
                return false;
            }
        }
        true
    }

    /// Vérifie que le mapping est bien un foncteur (respecte les domaines/codomaines).
    ///
    /// Pour chaque arête f: A → B dans S, on doit avoir :
    /// - Le chemin F(f) part de F(A) et arrive à F(B)
    pub fn validate(&self, source: &Schema, target: &Schema) -> Result<(), String> {
        // Vérifier la complétude
        if !self.is_complete(source) {
            return Err("Le mapping n'est pas complet : certains nœuds ou arêtes ne sont pas mappés".into());
        }

        // Vérifier que les nœuds cibles existent dans T
        for (src_node, tgt_node) in &self.node_mapping {
            if !target.nodes.contains_key(tgt_node) {
                return Err(format!(
                    "Le nœud cible '{}' (image de '{}') n'existe pas dans le schéma cible",
                    tgt_node, src_node
                ));
            }
        }

        // Vérifier que chaque arête est mappée de façon cohérente
        for (edge_name, edge_mapping) in &self.edge_mapping {
            let source_edge = source.edges.get(edge_name)
                .ok_or_else(|| format!("Arête '{}' n'existe pas dans le schéma source", edge_name))?;

            match source_edge {
                Edge::ForeignKey { source: src, target: tgt, .. } => {
                    // F(fk: A→B) doit être un chemin de F(A) vers F(B) dans T
                    let mapped_src = self.node_mapping.get(src)
                        .ok_or_else(|| format!("Nœud source '{}' de FK '{}' non mappé", src, edge_name))?;
                    let _mapped_tgt = self.node_mapping.get(tgt)
                        .ok_or_else(|| format!("Nœud cible '{}' de FK '{}' non mappé", tgt, edge_name))?;

                    match edge_mapping {
                        EdgeMapping::FkToPath(path) => {
                            if path.start != *mapped_src {
                                return Err(format!(
                                    "FK '{}': le chemin image commence à '{}' mais devrait commencer à '{}'",
                                    edge_name, path.start, mapped_src
                                ));
                            }
                            // TODO: vérifier que le chemin arrive bien à mapped_tgt
                        }
                        _ => return Err(format!("FK '{}' mappée comme attribut", edge_name)),
                    }
                }
                Edge::Attribute { source: src, .. } => {
                    let _mapped_src = self.node_mapping.get(src)
                        .ok_or_else(|| format!("Nœud source '{}' de attribut '{}' non mappé", src, edge_name))?;

                    match edge_mapping {
                        EdgeMapping::AttrToPath { .. } => {
                            // OK, attribut bien mappé vers un chemin+attribut
                        }
                        _ => return Err(format!("Attribut '{}' mappé comme FK", edge_name)),
                    }
                }
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for Mapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "mapping {} : {} -> {} = {{", self.name, self.source_schema_name, self.target_schema_name)?;
        
        writeln!(f, "  entities")?;
        for (src, tgt) in &self.node_mapping {
            writeln!(f, "    {} -> {}", src, tgt)?;
        }

        writeln!(f, "  edges")?;
        for (src, mapping) in &self.edge_mapping {
            match mapping {
                EdgeMapping::FkToPath(path) => {
                    writeln!(f, "    {} -> {}", src, path)?;
                }
                EdgeMapping::AttrToPath { fk_path, attr_name } => {
                    if fk_path.is_empty() {
                        writeln!(f, "    {} -> {}", src, attr_name)?;
                    } else {
                        writeln!(f, "    {} -> {}.{}", src, fk_path.join("."), attr_name)?;
                    }
                }
            }
        }

        write!(f, "}}")
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::BaseType;

    fn schema_old() -> Schema {
        let mut s = Schema::new("OldCompany");
        s.add_node("Person")
         .add_node("Dept")
         .add_fk("works_in", "Person", "Dept")
         .add_attribute("person_name", "Person", BaseType::String)
         .add_attribute("dept_name", "Dept", BaseType::String);
        s
    }

    fn schema_new() -> Schema {
        let mut s = Schema::new("NewCompany");
        s.add_node("Employee")
         .add_node("Department")
         .add_fk("department", "Employee", "Department")
         .add_attribute("emp_name", "Employee", BaseType::String)
         .add_attribute("dept_label", "Department", BaseType::String);
        s
    }

    #[test]
    fn test_create_mapping() {
        let s_old = schema_old();
        let s_new = schema_new();

        let mut m = Mapping::new("Rename", "OldCompany", "NewCompany");
        m.map_node("Person", "Employee")
         .map_node("Dept", "Department")
         .map_fk("works_in", Path::new("Employee", vec!["department"]))
         .map_attr_direct("person_name", "emp_name")
         .map_attr_direct("dept_name", "dept_label");

        assert!(m.is_complete(&s_old));
        assert!(m.validate(&s_old, &s_new).is_ok());
    }

    #[test]
    fn test_incomplete_mapping() {
        let s_old = schema_old();

        let mut m = Mapping::new("Partial", "OldCompany", "NewCompany");
        m.map_node("Person", "Employee");
        // On oublie de mapper "Dept" et les arêtes

        assert!(!m.is_complete(&s_old));
    }
}
