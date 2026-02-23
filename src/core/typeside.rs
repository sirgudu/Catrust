// =============================================================================
// TYPESIDE — Les types primitifs du système
// =============================================================================
//
// En CQL, un "Typeside" définit les types de base disponibles (String, Int, Bool...)
// ainsi que les opérations sur ces types (concaténation, addition...) et les
// équations qui les régissent (associativité, etc.).
//
// ANALOGIE : Le Typeside est comme les "briques élémentaires" au-dessus desquelles
// on construit tout le reste. C'est l'équivalent des types SQL (VARCHAR, INTEGER...)
// mais de façon abstraite et indépendante de tout backend.
//
// MATHÉMATIQUEMENT : Un Typeside est une théorie algébrique multi-sortée.
// - Les "sorts" sont les types (String, Int, Bool)
// - Les "opérations" sont des fonctions entre types (length: String → Int)
// - Les "équations" sont des lois (length("") = 0)
//
// =============================================================================

use std::collections::HashMap;
use std::fmt;

/// Un type de base dans le système CQL.
/// 
/// Chaque type correspond à un "sort" dans la théorie algébrique.
/// On peut l'étendre facilement pour supporter des types custom.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BaseType {
    /// Chaîne de caractères (→ VARCHAR en SQL, String en Neo4j)
    String,
    /// Entier (→ INTEGER en SQL, Integer en Neo4j)  
    Integer,
    /// Nombre à virgule flottante (→ DOUBLE en SQL, Float en Neo4j)
    Float,
    /// Booléen (→ BOOLEAN en SQL/Neo4j)
    Boolean,
    /// Type personnalisé défini par l'utilisateur
    Custom(std::string::String),
}

impl fmt::Display for BaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaseType::String => write!(f, "String"),
            BaseType::Integer => write!(f, "Int"),
            BaseType::Float => write!(f, "Float"),
            BaseType::Boolean => write!(f, "Bool"),
            BaseType::Custom(name) => write!(f, "{}", name),
        }
    }
}

/// Une valeur concrète dans le système.
/// 
/// Les valeurs sont les "éléments" des types. Quand on a des données
/// dans une Instance, chaque cellule contient une Value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(std::string::String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Value {
    /// Retourne le BaseType correspondant à cette valeur
    pub fn get_type(&self) -> BaseType {
        match self {
            Value::String(_) => BaseType::String,
            Value::Integer(_) => BaseType::Integer,
            Value::Float(_) => BaseType::Float,
            Value::Boolean(_) => BaseType::Boolean,
            Value::Null => BaseType::String, // Null est polymorphe, par défaut String
        }
    }
}

/// Signature d'une opération sur les types.
///
/// Par exemple : `length: String → Int` a input_types = [String], output_type = Int
#[derive(Debug, Clone)]
pub struct OpSignature {
    pub name: std::string::String,
    pub input_types: Vec<BaseType>,
    pub output_type: BaseType,
}

/// Le Typeside complet : ensemble des types + opérations disponibles.
///
/// C'est le "socle" sur lequel on construit les Schemas.
/// En CQL textuel, ça ressemble à :
///
/// ```cql
/// typeside Ty = literal {
///     types
///         String
///         Int
///     constants
///         Alice Bob : String
///         zero : Int
///     functions
///         length : String -> Int
///         plus : Int, Int -> Int
///     equations
///         forall x:Int. plus(x, zero) = x
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Typeside {
    /// Les types de base disponibles
    pub types: Vec<BaseType>,
    /// Les opérations (fonctions entre types)
    pub operations: Vec<OpSignature>,
    /// Constantes nommées : nom → (type, valeur)
    pub constants: HashMap<std::string::String, (BaseType, Value)>,
}

impl Typeside {
    /// Crée un Typeside par défaut avec les types standard (String, Int, Float, Bool).
    /// C'est le typeside "SQL" de base, suffisant pour la plupart des cas.
    pub fn default_sql() -> Self {
        Typeside {
            types: vec![
                BaseType::String,
                BaseType::Integer,
                BaseType::Float,
                BaseType::Boolean,
            ],
            operations: vec![],
            constants: HashMap::new(),
        }
    }

    /// Crée un Typeside vide (utile pour les tests)
    pub fn empty() -> Self {
        Typeside {
            types: vec![],
            operations: vec![],
            constants: HashMap::new(),
        }
    }

    /// Vérifie qu'un type existe dans ce Typeside
    pub fn has_type(&self, ty: &BaseType) -> bool {
        self.types.contains(ty)
    }

    /// Ajoute un type au Typeside
    pub fn add_type(&mut self, ty: BaseType) {
        if !self.types.contains(&ty) {
            self.types.push(ty);
        }
    }

    /// Ajoute une opération
    pub fn add_operation(&mut self, op: OpSignature) {
        self.operations.push(op);
    }
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_typeside() {
        let ts = Typeside::default_sql();
        assert!(ts.has_type(&BaseType::String));
        assert!(ts.has_type(&BaseType::Integer));
        assert!(ts.has_type(&BaseType::Float));
        assert!(ts.has_type(&BaseType::Boolean));
        assert!(!ts.has_type(&BaseType::Custom("Date".into())));
    }

    #[test]
    fn test_value_types() {
        let v = Value::String("hello".into());
        assert_eq!(v.get_type(), BaseType::String);
        
        let v = Value::Integer(42);
        assert_eq!(v.get_type(), BaseType::Integer);
    }

    #[test]
    fn test_add_custom_type() {
        let mut ts = Typeside::default_sql();
        ts.add_type(BaseType::Custom("Date".into()));
        assert!(ts.has_type(&BaseType::Custom("Date".into())));
    }
}
