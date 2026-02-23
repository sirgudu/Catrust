// =============================================================================
// OPTIMIZE — Réécriture et optimisation de chemins catégoriques
// =============================================================================
//
// Ce module est le cœur de l'optimisation. Il utilise les ÉQUATIONS DE CHEMINS
// du schéma pour simplifier les chemins (= éliminer des JOINs en SQL).
//
// IDÉE FONDAMENTALE :
//   Dans une catégorie, si on a l'équation `f.g.h = f.k`, alors le chemin
//   de longueur 3 (3 JOINs) peut être remplacé par un chemin de longueur 2
//   (2 JOINs). C'est une réécriture de termes guidée par la structure
//   catégorique.
//
// ALGORITHME :
//   1. Construire un système de réécriture à partir des path equations
//   2. Orienter les règles : le côté le plus long est réécrit vers le plus court
//   3. Appliquer les règles jusqu'à un point fixe (forme normale)
//   4. Le résultat est le chemin le plus court possible
//
// ANALOGIE SQL :
//   Path equation : employee.department.manager = employee.direct_manager
//   Avant optim   : SELECT ... FROM emp JOIN dept JOIN emp AS mgr  (2 JOINs)
//   Après optim   : SELECT ... FROM emp JOIN emp AS mgr            (1 JOIN)
//
// ANALOGIE MATHÉMATIQUE :
//   C'est exactement la complétion de Knuth-Bendix sur le monoïde libre
//   des chemins, quotienté par les path equations. On cherche un système
//   de réécriture convergent (confluent + terminant).
//
// =============================================================================

use super::schema::{Schema, Path, Edge};

/// Une règle de réécriture : on remplace `lhs` par `rhs` quand on trouve
/// `lhs` comme sous-chemin.
///
/// Convention : lhs est PLUS LONG (ou égal) que rhs.
/// Ainsi, chaque réécriture réduit strictement la longueur → terminaison.
#[derive(Debug, Clone)]
pub struct RewriteRule {
    /// Le pattern à chercher (le chemin long)
    pub lhs: Path,
    /// Le remplacement (le chemin court)
    pub rhs: Path,
    /// Nom de la règle (pour le debug / explication de l'optimisation)
    pub name: String,
}

impl std::fmt::Display for RewriteRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} ⟶ {}", self.name, self.lhs, self.rhs)
    }
}

/// Le résultat d'une optimisation de chemin.
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    /// Le chemin original
    pub original: Path,
    /// Le chemin optimisé (forme normale)
    pub optimized: Path,
    /// Les règles appliquées (dans l'ordre)
    pub rules_applied: Vec<String>,
    /// Nombre de JOINs éliminés
    pub joins_eliminated: usize,
}

impl std::fmt::Display for OptimizationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.joins_eliminated > 0 {
            writeln!(f, "Optimisation : {} → {}", self.original, self.optimized)?;
            writeln!(f, "  JOINs éliminés : {}", self.joins_eliminated)?;
            for rule in &self.rules_applied {
                writeln!(f, "  Règle appliquée : {}", rule)?;
            }
        } else {
            writeln!(f, "Chemin déjà optimal : {}", self.original)?;
        }
        Ok(())
    }
}

/// L'optimiseur de chemins.
///
/// Il construit un système de réécriture à partir des path equations
/// d'un schéma, puis l'applique pour simplifier tout chemin.
#[derive(Debug, Clone)]
pub struct PathOptimizer {
    /// Les règles de réécriture (dérivées des path equations)
    pub rules: Vec<RewriteRule>,
}

impl PathOptimizer {
    /// Construit un optimiseur à partir d'un schéma.
    ///
    /// Pour chaque path equation `lhs = rhs`, on crée une règle :
    /// - Si len(lhs) > len(rhs) : lhs → rhs (on raccourcit)
    /// - Si len(rhs) > len(lhs) : rhs → lhs (on raccourcit)
    /// - Si len(lhs) = len(rhs) : les deux directions (on choisit un ordre)
    pub fn from_schema(schema: &Schema) -> Self {
        let mut rules = Vec::new();

        for (i, eq) in schema.path_equations.iter().enumerate() {
            let rule_name = format!("eq_{}", i);

            if eq.lhs.len() > eq.rhs.len() {
                // lhs est plus long → le réécrire vers rhs
                rules.push(RewriteRule {
                    lhs: eq.lhs.clone(),
                    rhs: eq.rhs.clone(),
                    name: rule_name,
                });
            } else if eq.rhs.len() > eq.lhs.len() {
                // rhs est plus long → le réécrire vers lhs
                rules.push(RewriteRule {
                    lhs: eq.rhs.clone(),
                    rhs: eq.lhs.clone(),
                    name: rule_name,
                });
            } else {
                // Même longueur : on choisit un ordre lexicographique
                // (convention : on réécrit vers le "plus petit" lex.)
                let lhs_str = format!("{}", eq.lhs);
                let rhs_str = format!("{}", eq.rhs);
                if lhs_str > rhs_str {
                    rules.push(RewriteRule {
                        lhs: eq.lhs.clone(),
                        rhs: eq.rhs.clone(),
                        name: rule_name,
                    });
                } else {
                    rules.push(RewriteRule {
                        lhs: eq.rhs.clone(),
                        rhs: eq.lhs.clone(),
                        name: rule_name,
                    });
                }
            }
        }

        // Ajouter des règles dérivées par transitivité
        // Si A.f.g = A.h et A.h.k = A.m, alors A.f.g.k = A.m
        // On fait un passage pour détecter ces chaînes
        let derived = Self::derive_transitive_rules(&rules, schema);
        rules.extend(derived);

        PathOptimizer { rules }
    }

    /// Dérive des règles transitives.
    ///
    /// Si on a lhs1 → rhs1 et que rhs1 est un préfixe de lhs2,
    /// on peut créer une règle combinée.
    fn derive_transitive_rules(rules: &[RewriteRule], _schema: &Schema) -> Vec<RewriteRule> {
        let mut derived = Vec::new();

        for (i, r1) in rules.iter().enumerate() {
            for (j, r2) in rules.iter().enumerate() {
                if i == j { continue; }

                // Si rhs de r1 est un préfixe compatible avec lhs de r2
                // On peut composer les réécritures
                if r1.rhs.start == r2.lhs.start
                    && !r1.rhs.edges.is_empty()
                    && !r2.lhs.edges.is_empty()
                {
                    // Vérifier si les dernières arêtes de r1.rhs coincident
                    // avec les premières arêtes de r2.lhs
                    if r1.rhs.edges.last() == r2.lhs.edges.first()
                        && r1.rhs.edges == r2.lhs.edges[..r1.rhs.edges.len()]
                    {
                        // On peut étendre r1 : lhs1 + suffixe de lhs2 → rhs2
                        let suffix: Vec<_> = r2.lhs.edges[r1.rhs.edges.len()..].to_vec();
                        let mut extended_lhs = r1.lhs.clone();
                        extended_lhs.edges.extend(suffix);

                        let combined_len = extended_lhs.len();
                        let result_len = r2.rhs.len();

                        if combined_len > result_len {
                            derived.push(RewriteRule {
                                lhs: extended_lhs,
                                rhs: r2.rhs.clone(),
                                name: format!("derived_{}_{}", i, j),
                            });
                        }
                    }
                }
            }
        }

        derived
    }

    /// Optimise un chemin en appliquant les règles de réécriture
    /// jusqu'à atteindre un point fixe (forme normale).
    ///
    /// Retourne le chemin optimisé et la trace des règles appliquées.
    pub fn optimize(&self, path: &Path) -> OptimizationResult {
        let original = path.clone();
        let mut current = path.clone();
        let mut rules_applied = Vec::new();
        let mut changed = true;

        // Appliquer les règles jusqu'au point fixe
        // (terminaison garantie car chaque règle réduit la longueur ou est idempotente)
        let max_iterations = 100; // sécurité anti-boucle infinie
        let mut iteration = 0;

        while changed && iteration < max_iterations {
            changed = false;
            iteration += 1;

            for rule in &self.rules {
                if let Some(new_path) = self.apply_rule(&current, rule) {
                    if new_path.len() < current.len() || new_path != current {
                        rules_applied.push(format!("{}", rule));
                        current = new_path;
                        changed = true;
                        break; // Recommencer depuis le début après chaque réécriture
                    }
                }
            }
        }

        let joins_eliminated = if original.len() > current.len() {
            original.len() - current.len()
        } else {
            0
        };

        OptimizationResult {
            original,
            optimized: current,
            rules_applied,
            joins_eliminated,
        }
    }

    /// Tente d'appliquer une règle de réécriture à un chemin.
    ///
    /// Cherche le pattern `rule.lhs` comme sous-séquence contiguë
    /// dans le chemin, et le remplace par `rule.rhs`.
    fn apply_rule(&self, path: &Path, rule: &RewriteRule) -> Option<Path> {
        // Le chemin doit commencer au même nœud
        if path.start != rule.lhs.start {
            return None;
        }

        let pattern = &rule.lhs.edges;
        let target = &path.edges;

        if pattern.is_empty() || target.len() < pattern.len() {
            return None;
        }

        // Chercher le pattern comme sous-séquence contiguë
        for i in 0..=(target.len() - pattern.len()) {
            if target[i..i + pattern.len()] == pattern[..] {
                // Trouvé ! Remplacer par rule.rhs.edges
                let mut new_edges = Vec::new();
                // Préfixe avant le match
                new_edges.extend_from_slice(&target[..i]);
                // Remplacement
                new_edges.extend_from_slice(&rule.rhs.edges);
                // Suffixe après le match
                new_edges.extend_from_slice(&target[i + pattern.len()..]);

                return Some(Path {
                    start: path.start.clone(),
                    edges: new_edges,
                });
            }
        }

        None
    }

    /// Optimise un chemin et retourne uniquement le résultat (sans la trace).
    pub fn optimize_path(&self, path: &Path) -> Path {
        self.optimize(path).optimized
    }

    /// Retourne le nombre de JOINs qu'on peut éliminer pour ce chemin.
    pub fn joins_saved(&self, path: &Path) -> usize {
        self.optimize(path).joins_eliminated
    }

    /// Analyse un schéma et retourne un rapport d'optimisations possibles.
    ///
    /// Pour chaque paire (nœud source, nœud cible) atteignable dans le schéma,
    /// on cherche tous les chemins et on les optimise.
    pub fn analyze_schema(&self, schema: &Schema) -> Vec<OptimizationResult> {
        let mut results = Vec::new();

        // Pour chaque nœud, explorer les chemins de longueur 2+ et les optimiser
        for node_name in schema.nodes.keys() {
            let paths = enumerate_paths(schema, node_name, 4); // profondeur max 4
            for path in paths {
                let result = self.optimize(&path);
                if result.joins_eliminated > 0 {
                    results.push(result);
                }
            }
        }

        results
    }
}

/// Compose deux mappings pour éviter la matérialisation intermédiaire.
///
/// Si F : S → T et G : T → U, alors compose(F, G) : S → U
/// Ceci permet de générer une SEULE requête SQL au lieu de deux.
///
/// MATHÉMATIQUEMENT : c'est la composition de foncteurs G ∘ F.
///
/// GAIN SQL :
///   Sans composition : CREATE TEMP TABLE t AS (migration F); migration G depuis t;
///   Avec composition : une seule migration directe S → U.
pub fn compose_mappings(
    f: &super::mapping::Mapping,
    g: &super::mapping::Mapping,
    target_of_f: &Schema,  // = source de G
) -> Result<super::mapping::Mapping, String> {
    use super::mapping::{Mapping, EdgeMapping};

    // Vérifier que F.target = G.source
    if f.target_schema_name != g.source_schema_name {
        return Err(format!(
            "Incompatible : F cible '{}' ≠ G source '{}'",
            f.target_schema_name, g.source_schema_name
        ));
    }

    let mut composed = Mapping::new(
        &format!("{}∘{}", g.name, f.name),
        &f.source_schema_name,
        &g.target_schema_name,
    );

    // Composer les nœuds : (G∘F)(node) = G(F(node))
    for (src_node, mid_node) in &f.node_mapping {
        match g.node_mapping.get(mid_node) {
            Some(tgt_node) => {
                composed.map_node(src_node, tgt_node);
            }
            None => {
                return Err(format!(
                    "Nœud intermédiaire '{}' (image de '{}' par F) non mappé par G",
                    mid_node, src_node
                ));
            }
        }
    }

    // Composer les arêtes : (G∘F)(edge) = G(F(edge))
    for (src_edge, f_mapping) in &f.edge_mapping {
        match f_mapping {
            EdgeMapping::FkToPath(f_path) => {
                // F envoie cette FK sur un chemin dans T
                // On doit appliquer G à chaque arête de ce chemin
                let mut composed_edges = Vec::new();
                for mid_edge in &f_path.edges {
                    match g.edge_mapping.get(mid_edge) {
                        Some(EdgeMapping::FkToPath(g_path)) => {
                            composed_edges.extend(g_path.edges.clone());
                        }
                        Some(EdgeMapping::AttrToPath { .. }) => {
                            return Err(format!(
                                "FK '{}' dans F mène à l'arête '{}' qui est un attribut dans G",
                                src_edge, mid_edge
                            ));
                        }
                        None => {
                            return Err(format!(
                                "Arête '{}' (dans le chemin image de '{}') non mappée par G",
                                mid_edge, src_edge
                            ));
                        }
                    }
                }

                let start_in_u = composed.node_mapping
                    .get(&Path::new(&f_path.start, vec![]).start)
                    .or_else(|| {
                        // Trouver le nœud source dans S et son image dans U
                        f.node_mapping.get(src_edge)
                            .and_then(|mid| g.node_mapping.get(mid))
                    })
                    .cloned()
                    .unwrap_or_else(|| {
                        // Fallback : chercher via F puis G
                        let src_node = target_of_f.edges.get(&f_path.edges[0])
                            .map(|e| e.source().to_string())
                            .unwrap_or_default();
                        g.node_mapping.get(&src_node)
                            .cloned()
                            .unwrap_or(f_path.start.clone())
                    });

                composed.map_fk(
                    src_edge,
                    Path {
                        start: start_in_u,
                        edges: composed_edges,
                    },
                );
            }
            EdgeMapping::AttrToPath { fk_path, attr_name } => {
                // F envoie cet attribut vers un chemin FK + attribut dans T
                // On doit : 1) composer les FK via G, 2) composer l'attribut via G
                let mut composed_fk_path = Vec::new();
                for mid_fk in fk_path {
                    match g.edge_mapping.get(mid_fk) {
                        Some(EdgeMapping::FkToPath(g_path)) => {
                            composed_fk_path.extend(g_path.edges.clone());
                        }
                        _ => {
                            return Err(format!(
                                "FK '{}' dans le chemin de l'attribut '{}' non mappée par G",
                                mid_fk, src_edge
                            ));
                        }
                    }
                }

                // L'attribut final
                match g.edge_mapping.get(attr_name) {
                    Some(EdgeMapping::AttrToPath { fk_path: g_fk, attr_name: g_attr }) => {
                        composed_fk_path.extend(g_fk.clone());
                        composed.map_attr(
                            src_edge,
                            composed_fk_path.iter().map(|s| s.as_str()).collect(),
                            g_attr,
                        );
                    }
                    _ => {
                        return Err(format!(
                            "Attribut '{}' (image de '{}') non mappé comme attribut par G",
                            attr_name, src_edge
                        ));
                    }
                }
            }
        }
    }

    Ok(composed)
}

/// Énumère tous les chemins depuis un nœud donné, jusqu'à une profondeur max.
/// Utile pour l'analyse d'optimisation.
fn enumerate_paths(schema: &Schema, start: &str, max_depth: usize) -> Vec<Path> {
    let mut paths = Vec::new();
    let mut stack: Vec<(String, Vec<String>)> = vec![(start.to_string(), vec![])];

    while let Some((current_node, current_edges)) = stack.pop() {
        if current_edges.len() >= max_depth {
            continue;
        }

        // Trouver les FK sortantes du nœud courant
        for edge in schema.edges.values() {
            if let Edge::ForeignKey { name, source, target } = edge {
                if source == &current_node {
                    let mut new_edges = current_edges.clone();
                    new_edges.push(name.clone());

                    // Ajouter ce chemin s'il a au moins 2 arêtes
                    if new_edges.len() >= 2 {
                        paths.push(Path {
                            start: start.to_string(),
                            edges: new_edges.clone(),
                        });
                    }

                    // Continuer l'exploration
                    stack.push((target.clone(), new_edges));
                }
            }
        }
    }

    paths
}

// =============================================================================
// TESTS
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::typeside::BaseType;

    /// Schéma avec une équation de chemins optimisable :
    ///
    ///   Employee --department--> Department --manager--> Employee
    ///   Employee --direct_mgr--> Employee
    ///
    ///   Équation : Employee.department.manager = Employee.direct_mgr
    ///
    /// Cette équation signifie : "le manager du département = le manager direct"
    /// Elle permet d'éliminer 1 JOIN !
    fn schema_with_shortcut() -> Schema {
        let mut s = Schema::new("Company");
        s.add_node("Employee")
         .add_node("Department")
         .add_fk("department", "Employee", "Department")
         .add_fk("manager", "Department", "Employee")
         .add_fk("direct_mgr", "Employee", "Employee")
         .add_attribute("emp_name", "Employee", BaseType::String)
         .add_attribute("dept_name", "Department", BaseType::String)
         .add_path_equation(
             Path::new("Employee", vec!["department", "manager"]),
             Path::new("Employee", vec!["direct_mgr"]),
         );
        s
    }

    #[test]
    fn test_optimizer_eliminates_join() {
        let schema = schema_with_shortcut();
        let optimizer = PathOptimizer::from_schema(&schema);

        // Le chemin long : Employee.department.manager (2 JOINs)
        let long_path = Path::new("Employee", vec!["department", "manager"]);
        let result = optimizer.optimize(&long_path);

        // Doit être optimisé en : Employee.direct_mgr (1 JOIN)
        assert_eq!(result.optimized.edges, vec!["direct_mgr"]);
        assert_eq!(result.joins_eliminated, 1);
        println!("{}", result);
    }

    #[test]
    fn test_optimizer_already_optimal() {
        let schema = schema_with_shortcut();
        let optimizer = PathOptimizer::from_schema(&schema);

        // Un chemin qui est déjà court
        let short_path = Path::new("Employee", vec!["direct_mgr"]);
        let result = optimizer.optimize(&short_path);

        assert_eq!(result.joins_eliminated, 0);
        assert_eq!(result.optimized.edges, vec!["direct_mgr"]);
    }

    #[test]
    fn test_optimizer_extended_path() {
        let schema = schema_with_shortcut();
        let optimizer = PathOptimizer::from_schema(&schema);

        // Chemin : Employee.department.manager.department (3 JOINs)
        // Devrait devenir : Employee.direct_mgr.department (2 JOINs)
        let path = Path::new("Employee", vec!["department", "manager", "department"]);
        let result = optimizer.optimize(&path);

        assert_eq!(result.optimized.edges, vec!["direct_mgr", "department"]);
        assert_eq!(result.joins_eliminated, 1);
        println!("{}", result);
    }

    #[test]
    fn test_optimizer_double_shortcut() {
        let schema = schema_with_shortcut();
        let optimizer = PathOptimizer::from_schema(&schema);

        // Chemin : Employee.department.manager.department.manager (4 JOINs)
        // 1ère passe : Employee.direct_mgr.department.manager (3 JOINs)
        // 2ème passe : Employee.direct_mgr.direct_mgr (2 JOINs)
        let path = Path::new("Employee", vec![
            "department", "manager", "department", "manager"
        ]);
        let result = optimizer.optimize(&path);

        assert_eq!(result.optimized.edges, vec!["direct_mgr", "direct_mgr"]);
        assert_eq!(result.joins_eliminated, 2);
        println!("{}", result);
    }

    #[test]
    fn test_analyze_schema() {
        let schema = schema_with_shortcut();
        let optimizer = PathOptimizer::from_schema(&schema);
        let analysis = optimizer.analyze_schema(&schema);

        assert!(!analysis.is_empty(), "Devrait trouver des optimisations");
        for result in &analysis {
            println!("{}", result);
        }
    }

    #[test]
    fn test_compose_mappings() {
        use crate::core::mapping::Mapping;

        // S → T → U
        // S: Person, Dept
        // T: Employee, Department  
        // U: Worker, Unit

        let mut schema_s = Schema::new("S");
        schema_s.add_node("Person").add_node("Dept")
            .add_fk("works_in", "Person", "Dept")
            .add_attribute("pname", "Person", BaseType::String)
            .add_attribute("dname", "Dept", BaseType::String);

        let mut schema_t = Schema::new("T");
        schema_t.add_node("Employee").add_node("Department")
            .add_fk("dept", "Employee", "Department")
            .add_attribute("ename", "Employee", BaseType::String)
            .add_attribute("dlabel", "Department", BaseType::String);

        let mut schema_u = Schema::new("U");
        schema_u.add_node("Worker").add_node("Unit")
            .add_fk("unit", "Worker", "Unit")
            .add_attribute("wname", "Worker", BaseType::String)
            .add_attribute("uname", "Unit", BaseType::String);

        let mut f = Mapping::new("F", "S", "T");
        f.map_node("Person", "Employee")
         .map_node("Dept", "Department")
         .map_fk("works_in", Path::new("Employee", vec!["dept"]))
         .map_attr_direct("pname", "ename")
         .map_attr_direct("dname", "dlabel");

        let mut g = Mapping::new("G", "T", "U");
        g.map_node("Employee", "Worker")
         .map_node("Department", "Unit")
         .map_fk("dept", Path::new("Worker", vec!["unit"]))
         .map_attr_direct("ename", "wname")
         .map_attr_direct("dlabel", "uname");

        let composed = compose_mappings(&f, &g, &schema_t).unwrap();

        assert_eq!(composed.node_mapping.get("Person").unwrap(), "Worker");
        assert_eq!(composed.node_mapping.get("Dept").unwrap(), "Unit");
        println!("{}", composed);
    }
}
