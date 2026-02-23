// =============================================================================
// INSTANCE — Un foncteur Schema → Set (les données concrètes)
// =============================================================================
//
// En théorie des catégories, une INSTANCE d'un Schema S est un FONCTEUR :
//   I : S → Set
//
// Concrètement, ça veut dire :
//   - Pour chaque NŒUD (entité/table), I assigne un ENSEMBLE de lignes
//   - Pour chaque ARÊTE FK, I assigne une FONCTION qui dit où pointe la FK
//   - Pour chaque ARÊTE Attribut, I assigne une FONCTION qui donne la valeur
//
// ANALOGIE : Si le Schema est le "moule" (CREATE TABLE), l'Instance est le
// "contenu" (les INSERT INTO). Mais avec une structure catégorique rigoureuse.
//
// EXEMPLE :
//   Schema: Employee --works_in--> Department
//                |                      |
//              emp_name              dept_name
//                |                      |
//                v                      v
//              String                 String
//
//   Instance:
//     Employee = { e1, e2, e3 }
//     Department = { d1, d2 }
//     works_in(e1) = d1, works_in(e2) = d1, works_in(e3) = d2
//     emp_name(e1) = "Alice", emp_name(e2) = "Bob", emp_name(e3) = "Charlie"
//     dept_name(d1) = "Engineering", dept_name(d2) = "Marketing"
//
// PROPRIÉTÉ FONCTORIELLE : I doit respecter la composition.
//   Si on a un chemin A --f--> B --g--> C, alors I(g∘f) = I(g) ∘ I(f)
//   C'est-à-dire : suivre la FK PUIS lire l'attribut = lire directement
//
// =============================================================================

use std::collections::HashMap;
use super::typeside::Value;
use super::schema::Schema;

/// Identifiant unique d'une ligne dans une table.
/// 
/// Chaque ligne a un ID unique au sein de son entité.
/// C'est un identifiant interne au moteur, pas nécessairement visible.
pub type RowId = u64;

/// Les données d'une entité (table) : un ensemble de lignes.
///
/// Chaque ligne est identifiée par un RowId, et contient une
/// HashMap de valeurs (attribut_name → Value).
///
/// Le champ `fk_values` stocke les FK : pour chaque FK sortante,
/// on sait vers quel RowId de l'entité cible cette ligne pointe.
#[derive(Debug, Clone)]
pub struct EntityData {
    /// Compteur pour générer les RowId auto-incrémentés
    next_id: RowId,
    /// Les valeurs d'attributs : row_id → (attr_name → Value)
    pub attribute_values: HashMap<RowId, HashMap<String, Value>>,
    /// Les valeurs de FK : row_id → (fk_name → RowId cible)
    pub fk_values: HashMap<RowId, HashMap<String, RowId>>,
}

impl EntityData {
    pub fn new() -> Self {
        EntityData {
            next_id: 1,
            attribute_values: HashMap::new(),
            fk_values: HashMap::new(),
        }
    }

    /// Insère une nouvelle ligne avec ses attributs et FK.
    /// Retourne le RowId attribué.
    ///
    /// `attrs` : les valeurs d'attributs (nom → valeur)
    /// `fks` : les FK (nom_fk → RowId cible)
    pub fn insert(
        &mut self,
        attrs: HashMap<String, Value>,
        fks: HashMap<String, RowId>,
    ) -> RowId {
        let id = self.next_id;
        self.next_id += 1;
        self.attribute_values.insert(id, attrs);
        self.fk_values.insert(id, fks);
        id
    }

    /// Insère une ligne avec un RowId spécifique (utile pour les migrations)
    pub fn insert_with_id(
        &mut self,
        id: RowId,
        attrs: HashMap<String, Value>,
        fks: HashMap<String, RowId>,
    ) {
        if id >= self.next_id {
            self.next_id = id + 1;
        }
        self.attribute_values.insert(id, attrs);
        self.fk_values.insert(id, fks);
    }

    /// Nombre de lignes dans cette entité
    pub fn len(&self) -> usize {
        self.attribute_values.len()
    }

    /// L'entité est-elle vide ?
    pub fn is_empty(&self) -> bool {
        self.attribute_values.is_empty()
    }

    /// Retourne tous les RowId
    pub fn row_ids(&self) -> Vec<RowId> {
        self.attribute_values.keys().copied().collect()
    }

    /// Lit la valeur d'un attribut pour une ligne donnée
    pub fn get_attr(&self, row_id: RowId, attr_name: &str) -> Option<&Value> {
        self.attribute_values
            .get(&row_id)
            .and_then(|attrs| attrs.get(attr_name))
    }

    /// Lit la cible d'une FK pour une ligne donnée
    pub fn get_fk(&self, row_id: RowId, fk_name: &str) -> Option<RowId> {
        self.fk_values
            .get(&row_id)
            .and_then(|fks| fks.get(fk_name))
            .copied()
    }
}

/// Instance complète : un foncteur Schema → Set.
///
/// Pour chaque entité du Schema, on a un EntityData.
/// L'Instance doit respecter la propriété fonctorielle :
/// les FK forment des fonctions bien définies entre les ensembles.
#[derive(Debug, Clone)]
pub struct Instance {
    /// Nom de cette instance
    pub name: String,
    /// Nom du schéma associé
    pub schema_name: String,
    /// Données par entité : entity_name → EntityData
    pub data: HashMap<String, EntityData>,
}

impl Instance {
    /// Crée une instance vide pour un schéma donné
    pub fn new(name: &str, schema: &Schema) -> Self {
        let mut data = HashMap::new();
        for node_name in schema.nodes.keys() {
            data.insert(node_name.clone(), EntityData::new());
        }
        Instance {
            name: name.to_string(),
            schema_name: schema.name.clone(),
            data,
        }
    }

    /// Insère une ligne dans une entité.
    /// Retourne le RowId attribué.
    pub fn insert(
        &mut self,
        entity: &str,
        attrs: HashMap<String, Value>,
        fks: HashMap<String, RowId>,
    ) -> RowId {
        self.data
            .get_mut(entity)
            .unwrap_or_else(|| panic!("Entité '{}' n'existe pas dans l'instance", entity))
            .insert(attrs, fks)
    }

    /// Évalue un chemin (séquence de FK) depuis un RowId de départ.
    ///
    /// C'est l'APPLICATION du foncteur à un morphisme composé.
    /// Si le chemin est [fk1, fk2], on suit : row --fk1--> row' --fk2--> row''
    ///
    /// C'est exactement la propriété fonctorielle : I(g∘f) = I(g)∘I(f)
    pub fn follow_path(
        &self,
        start_entity: &str,
        start_row: RowId,
        fk_path: &[String],
        schema: &Schema,
    ) -> Option<RowId> {
        let mut current_entity = start_entity.to_string();
        let mut current_row = start_row;

        for fk_name in fk_path {
            // Trouver l'arête FK et sa cible
            let edge = schema.edges.get(fk_name)?;
            match edge {
                super::schema::Edge::ForeignKey { target, .. } => {
                    current_row = self.data
                        .get(&current_entity)?
                        .get_fk(current_row, fk_name)?;
                    current_entity = target.clone();
                }
                _ => return None, // Ce n'est pas une FK
            }
        }
        Some(current_row)
    }

    /// Nombre total de lignes dans toutes les entités
    pub fn total_rows(&self) -> usize {
        self.data.values().map(|ed| ed.len()).sum()
    }

    /// Affiche l'instance de manière lisible (pour le debug)
    pub fn display(&self, schema: &Schema) -> String {
        let mut out = format!("instance {} : {} = {{\n", self.name, self.schema_name);

        for (entity_name, entity_data) in &self.data {
            if entity_data.is_empty() {
                continue;
            }
            out.push_str(&format!("  {} ({} lignes):\n", entity_name, entity_data.len()));

            for row_id in entity_data.row_ids() {
                out.push_str(&format!("    [{}]", row_id));

                // Attributs
                if let Some(attrs) = entity_data.attribute_values.get(&row_id) {
                    for (attr_name, value) in attrs {
                        out.push_str(&format!(" {}: {},", attr_name, value));
                    }
                }

                // FK
                if let Some(fks) = entity_data.fk_values.get(&row_id) {
                    for (fk_name, target_id) in fks {
                        // Trouver le nom de l'entité cible
                        let target_entity = if let Some(edge) = schema.edges.get(fk_name) {
                            match edge {
                                super::schema::Edge::ForeignKey { target, .. } => target.clone(),
                                _ => "?".to_string(),
                            }
                        } else {
                            "?".to_string()
                        };
                        out.push_str(&format!(" {} -> {}[{}],", fk_name, target_entity, target_id));
                    }
                }

                out.push('\n');
            }
        }

        out.push_str("}\n");
        out
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::schema::Schema;
    use crate::core::typeside::BaseType;

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

    fn company_instance(schema: &Schema) -> Instance {
        let mut inst = Instance::new("CompanyData", schema);

        // Insérer des départements
        let d1 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Engineering".into()))]),
            HashMap::new(),
        );
        let d2 = inst.insert("Department",
            HashMap::from([("dept_name".into(), Value::String("Marketing".into()))]),
            HashMap::new(),
        );

        // Insérer des employés
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Alice".into())),
                ("salary".into(), Value::Integer(80000)),
            ]),
            HashMap::from([("works_in".into(), d1)]),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Bob".into())),
                ("salary".into(), Value::Integer(75000)),
            ]),
            HashMap::from([("works_in".into(), d1)]),
        );
        inst.insert("Employee",
            HashMap::from([
                ("emp_name".into(), Value::String("Charlie".into())),
                ("salary".into(), Value::Integer(90000)),
            ]),
            HashMap::from([("works_in".into(), d2)]),
        );

        inst
    }

    #[test]
    fn test_create_instance() {
        let schema = company_schema();
        let inst = company_instance(&schema);
        assert_eq!(inst.total_rows(), 5); // 2 dept + 3 emp
        assert_eq!(inst.data["Employee"].len(), 3);
        assert_eq!(inst.data["Department"].len(), 2);
    }

    #[test]
    fn test_follow_fk() {
        let schema = company_schema();
        let inst = company_instance(&schema);

        // L'employé e1 (RowId=1 dans Employee) travaille dans d1 (RowId=1 dans Department)
        // Mais les RowId dépendent de l'ordre... on teste via follow_path
        let emp_rows = inst.data["Employee"].row_ids();
        let first_emp = emp_rows[0];
        let dept_id = inst.data["Employee"].get_fk(first_emp, "works_in").unwrap();
        assert!(inst.data["Department"].attribute_values.contains_key(&dept_id));
    }

    #[test]
    fn test_follow_path() {
        let schema = company_schema();
        let inst = company_instance(&schema);

        let emp_rows = inst.data["Employee"].row_ids();
        let first_emp = emp_rows[0];

        // Suivre le chemin Employee --works_in--> Department
        let result = inst.follow_path(
            "Employee", first_emp,
            &["works_in".to_string()],
            &schema,
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_display() {
        let schema = company_schema();
        let inst = company_instance(&schema);
        let display = inst.display(&schema);
        assert!(display.contains("Employee"));
        assert!(display.contains("Department"));
    }
}
