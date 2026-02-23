// =============================================================================
// SCHEMA — La catégorie qui décrit la structure des données
// =============================================================================
//
// En CQL, un Schema est UNE CATÉGORIE :
//   - Les OBJETS sont les entités (= tables en SQL, labels en Neo4j)
//   - Les MORPHISMES (arêtes) sont de deux types :
//       1. Foreign Keys (FK) : arêtes entre entités (Employee → Department)
//       2. Attributs : arêtes d'une entité vers un type (Employee → String pour le nom)
//   - Les ÉQUATIONS DE CHEMINS expriment des contraintes
//       Ex: employee.department.manager = employee.manager 
//       signifie que deux chemins dans la catégorie sont égaux.
//
// ANALOGIE : Imagine un diagramme avec des boîtes (tables) et des flèches
// (foreign keys et colonnes). Le Schema capture exactement cette structure.
//
// EXEMPLE VISUEL :
//
//   Employee ──department──▶ Department
//      │                        │
//      │name                    │name
//      ▼                        ▼
//    String                   String
//
// En CQL textuel :
//
// ```cql
// schema Company = literal : Ty {
//     entities
//         Employee
//         Department
//     foreign_keys
//         department : Employee -> Department
//     attributes
//         employee_name : Employee -> String
//         dept_name : Department -> String
//     observation_equations
//         // aucune dans cet exemple simple
// }
// ```
//
// =============================================================================

use std::collections::HashMap;
use super::typeside::BaseType;

/// Un nœud dans la catégorie-schéma = une entité = une table.
/// 
/// Identifié par son nom. Simple mais puissant : chaque Node est un objet
/// dans la catégorie.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
    pub name: String,
}

impl Node {
    pub fn new(name: &str) -> Self {
        Node { name: name.to_string() }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Une arête (morphisme) dans la catégorie-schéma.
///
/// Il y a deux sortes d'arêtes :
/// - **ForeignKey** : relie deux entités (ex: Employee → Department)
///   C'est un vrai morphisme dans la catégorie.
/// - **Attribute** : relie une entité à un type de base (ex: Employee → String)
///   C'est un morphisme vers un objet "type" dans la catégorie.
///
/// En SQL : FK = FOREIGN KEY, Attribute = colonne typée
/// En Neo4j : FK = relation, Attribute = propriété du nœud
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Edge {
    /// Clé étrangère : entité source → entité cible
    ForeignKey {
        name: String,
        source: String,  // nom du Node source
        target: String,  // nom du Node cible
    },
    /// Attribut : entité → type de base
    Attribute {
        name: String,
        source: String,      // nom du Node source
        target: BaseType,    // type de la valeur
    },
}

impl Edge {
    /// Crée une nouvelle Foreign Key
    pub fn fk(name: &str, source: &str, target: &str) -> Self {
        Edge::ForeignKey {
            name: name.to_string(),
            source: source.to_string(),
            target: target.to_string(),
        }
    }

    /// Crée un nouvel Attribut
    pub fn attr(name: &str, source: &str, target: BaseType) -> Self {
        Edge::Attribute {
            name: name.to_string(),
            source: source.to_string(),
            target,
        }
    }

    /// Retourne le nom de l'arête
    pub fn name(&self) -> &str {
        match self {
            Edge::ForeignKey { name, .. } => name,
            Edge::Attribute { name, .. } => name,
        }
    }

    /// Retourne le nom du nœud source
    pub fn source(&self) -> &str {
        match self {
            Edge::ForeignKey { source, .. } => source,
            Edge::Attribute { source, .. } => source,
        }
    }
}

/// Un chemin dans la catégorie = une séquence de morphismes composés.
///
/// Par exemple le chemin `employee.department.name` est :
///   Path { start: "Employee", edges: ["department", "name"] }
///
/// Ceci correspond à la COMPOSITION de morphismes en théorie des catégories :
///   name ∘ department : Employee → String
///
/// En SQL, un chemin = une chaîne de JOINs.
/// En Neo4j, un chemin = un pattern Cypher (a)-[:R1]->(b)-[:R2]->(c).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    /// Nœud de départ du chemin
    pub start: String,
    /// Séquence des noms d'arêtes à traverser
    pub edges: Vec<String>,
}

impl Path {
    pub fn new(start: &str, edges: Vec<&str>) -> Self {
        Path {
            start: start.to_string(),
            edges: edges.into_iter().map(|e| e.to_string()).collect(),
        }
    }

    /// Chemin identité (ne traverse aucune arête)
    pub fn identity(node: &str) -> Self {
        Path {
            start: node.to_string(),
            edges: vec![],
        }
    }

    /// Compose deux chemins (le second commence là où le premier finit)
    /// En catégorie : si f: A→B et g: B→C, alors g∘f: A→C
    pub fn compose(&self, other: &Path) -> Self {
        let mut edges = self.edges.clone();
        edges.extend(other.edges.clone());
        Path {
            start: self.start.clone(),
            edges,
        }
    }

    /// Longueur du chemin (nombre d'arêtes)
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Le chemin est-il un chemin identité ?
    pub fn is_identity(&self) -> bool {
        self.edges.is_empty()
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.edges.is_empty() {
            write!(f, "id_{}", self.start)
        } else {
            write!(f, "{}.{}", self.start, self.edges.join("."))
        }
    }
}

/// Équation de chemins : deux chemins qui doivent être égaux.
///
/// C'est LA contrainte fondamentale en CQL. Elle dit que deux façons
/// différentes de naviguer dans le schéma mènent au même résultat.
///
/// EXEMPLE :
///   `employee.department.manager = employee.direct_manager`
///   signifie : "le manager du département d'un employé EST le manager direct"
///
/// En SQL, ça se traduit par une contrainte CHECK ou un trigger.
/// En Neo4j, ça se vérifie par une requête MATCH.
///
/// MATHÉMATIQUEMENT : c'est un quotient de la catégorie libre engendrée
/// par le graphe sous-jacent, par les relations d'équivalence données.
#[derive(Debug, Clone)]
pub struct PathEquation {
    pub lhs: Path,  // côté gauche
    pub rhs: Path,  // côté droit
}

impl PathEquation {
    pub fn new(lhs: Path, rhs: Path) -> Self {
        PathEquation { lhs, rhs }
    }
}

impl std::fmt::Display for PathEquation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.lhs, self.rhs)
    }
}

/// Le Schema complet : une catégorie finiment présentée.
///
/// C'est la structure centrale de CQL. Un Schema contient :
/// - Des nœuds (entités/tables)
/// - Des arêtes (FK et attributs)  
/// - Des équations de chemins (contraintes)
///
/// Associé à un Typeside qui définit les types de base disponibles.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Nom du schéma
    pub name: String,
    /// Les nœuds (entités) : nom → Node
    pub nodes: HashMap<String, Node>,
    /// Les arêtes (FK + attributs) : nom → Edge
    pub edges: HashMap<String, Edge>,
    /// Les équations de chemins (contraintes catégoriques)
    pub path_equations: Vec<PathEquation>,
}

impl Schema {
    /// Crée un nouveau Schema vide
    pub fn new(name: &str) -> Self {
        Schema {
            name: name.to_string(),
            nodes: HashMap::new(),
            edges: HashMap::new(),
            path_equations: Vec::new(),
        }
    }

    /// Ajoute un nœud (entité/table) au schéma
    pub fn add_node(&mut self, name: &str) -> &mut Self {
        self.nodes.insert(name.to_string(), Node::new(name));
        self
    }

    /// Ajoute une Foreign Key (arête entre entités)
    pub fn add_fk(&mut self, name: &str, source: &str, target: &str) -> &mut Self {
        assert!(self.nodes.contains_key(source), 
            "Nœud source '{}' n'existe pas dans le schéma", source);
        assert!(self.nodes.contains_key(target), 
            "Nœud cible '{}' n'existe pas dans le schéma", target);
        self.edges.insert(name.to_string(), Edge::fk(name, source, target));
        self
    }

    /// Ajoute un attribut (arête vers un type de base)
    pub fn add_attribute(&mut self, name: &str, source: &str, ty: BaseType) -> &mut Self {
        assert!(self.nodes.contains_key(source),
            "Nœud source '{}' n'existe pas dans le schéma", source);
        self.edges.insert(name.to_string(), Edge::attr(name, source, ty));
        self
    }

    /// Ajoute une équation de chemins (contrainte)
    pub fn add_path_equation(&mut self, lhs: Path, rhs: Path) -> &mut Self {
        self.path_equations.push(PathEquation::new(lhs, rhs));
        self
    }

    /// Retourne toutes les Foreign Keys du schéma
    pub fn foreign_keys(&self) -> Vec<&Edge> {
        self.edges.values()
            .filter(|e| matches!(e, Edge::ForeignKey { .. }))
            .collect()
    }

    /// Retourne tous les attributs du schéma
    pub fn attributes(&self) -> Vec<&Edge> {
        self.edges.values()
            .filter(|e| matches!(e, Edge::Attribute { .. }))
            .collect()
    }

    /// Retourne les arêtes sortant d'un nœud donné
    pub fn edges_from(&self, node_name: &str) -> Vec<&Edge> {
        self.edges.values()
            .filter(|e| e.source() == node_name)
            .collect()
    }

    /// Retourne les FK pointant vers un nœud donné
    pub fn fks_targeting(&self, node_name: &str) -> Vec<&Edge> {
        self.edges.values()
            .filter(|e| {
                if let Edge::ForeignKey { target, .. } = e {
                    target == node_name
                } else {
                    false
                }
            })
            .collect()
    }

    /// Retourne les attributs d'un nœud donné
    pub fn attributes_of(&self, node_name: &str) -> Vec<&Edge> {
        self.edges.values()
            .filter(|e| {
                if let Edge::Attribute { source, .. } = e {
                    source == node_name
                } else {
                    false
                }
            })
            .collect()
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "schema {} = literal {{", self.name)?;
        
        writeln!(f, "  entities")?;
        for node in self.nodes.keys() {
            writeln!(f, "    {}", node)?;
        }
        
        let fks: Vec<_> = self.foreign_keys();
        if !fks.is_empty() {
            writeln!(f, "  foreign_keys")?;
            for fk in fks {
                if let Edge::ForeignKey { name, source, target } = fk {
                    writeln!(f, "    {} : {} -> {}", name, source, target)?;
                }
            }
        }

        let attrs: Vec<_> = self.attributes();
        if !attrs.is_empty() {
            writeln!(f, "  attributes")?;
            for attr in attrs {
                if let Edge::Attribute { name, source, target } = attr {
                    writeln!(f, "    {} : {} -> {}", name, source, target)?;
                }
            }
        }

        if !self.path_equations.is_empty() {
            writeln!(f, "  path_equations")?;
            for eq in &self.path_equations {
                writeln!(f, "    {}", eq)?;
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

    /// Crée le schéma classique Company pour les tests
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
    fn test_create_schema() {
        let s = company_schema();
        assert_eq!(s.nodes.len(), 2);
        assert_eq!(s.edges.len(), 4); // 1 FK + 3 attributs
    }

    #[test]
    fn test_foreign_keys() {
        let s = company_schema();
        let fks = s.foreign_keys();
        assert_eq!(fks.len(), 1);
    }

    #[test]
    fn test_attributes_of() {
        let s = company_schema();
        let emp_attrs = s.attributes_of("Employee");
        assert_eq!(emp_attrs.len(), 2); // emp_name, salary
    }

    #[test]
    fn test_path_display() {
        let p = Path::new("Employee", vec!["works_in", "dept_name"]);
        assert_eq!(format!("{}", p), "Employee.works_in.dept_name");
    }

    #[test]
    fn test_path_compose() {
        let p1 = Path::new("Employee", vec!["works_in"]);
        let p2 = Path::new("Department", vec!["dept_name"]);
        let composed = p1.compose(&p2);
        assert_eq!(composed.edges, vec!["works_in", "dept_name"]);
    }

    #[test]
    fn test_schema_display() {
        let s = company_schema();
        let display = format!("{}", s);
        assert!(display.contains("schema Company"));
        assert!(display.contains("Employee"));
        assert!(display.contains("Department"));
    }
}
