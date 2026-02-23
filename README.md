# Catrust — Categorical Query Language Engine in Rust

Moteur de requêtes et de migrations de données fondé sur la **théorie des catégories**, implémentant le langage **CQL** au-dessus de bases de données existantes (PostgreSQL, Snowflake, Trino, Neo4j).

## Architecture

```
src/
├── core/                    ← Cœur catégorique pur (zéro dépendance DB)
│   ├── typeside.rs          ── Types primitifs (String, Int, Bool...)
│   ├── schema.rs            ── Catégorie = nœuds + arêtes + équations de chemins
│   ├── instance.rs          ── Foncteur Schema → Set (les données)
│   ├── mapping.rs           ── Foncteur entre schémas (restructuration)
│   ├── migrate.rs           ── Δ (pullback), Σ (pushforward)
│   └── validate.rs          ── Vérification de cohérence catégorique
├── backend/                 ← Traduction vers les DB réelles
│   ├── sql/mod.rs           ── PostgreSQL, Snowflake, Trino
│   └── graph/mod.rs         ── Neo4j (Cypher)
├── lib.rs
└── main.rs                  ← Démo complète
```

## Concepts-clés

| CQL (catégorie)         | Base de données         | Rust (Catrust)           |
|-------------------------|-------------------------|--------------------------|
| **Schema** (catégorie)  | Tables + FK + colonnes  | `Schema`                 |
| **Instance** (foncteur) | Les données (rows)      | `Instance`               |
| **Mapping** (foncteur)  | Restructuration         | `Mapping`                |
| **Δ** (pullback)        | SELECT ... JOIN          | `migrate::delta()`       |
| **Σ** (left Kan ext.)   | INSERT INTO ... SELECT   | `migrate::sigma()`       |

---

## Références — par où commencer et dans quel ordre

### 🟢 Niveau 1 — Comprendre les catégories (prérequis)

Ces livres t'amènent de zéro à une compréhension solide de la théorie des catégories.

1. **Lawvere & Schanuel — *Conceptual Mathematics: A First Introduction to Categories*** (Cambridge, 2009)
   - LE point d'entrée. Aucun prérequis mathématique. Écrit comme une conversation.
   - Couvre : ensembles, fonctions, catégories, foncteurs, transformations naturelles.
   - Lire les parties I à III suffit pour comprendre CQL.

2. **Goldblatt — *Topoi: The Categorial Analysis of Logic*** (Dover, 2006)
   - Excellent pont entre logique, catégories et topoi.
   - Les **chapitres 1 à 6** donnent les bases catégoriques (catégories, foncteurs, transformations naturelles, limites, colimites, adjonctions).
   - Les chapitres suivants (sur les topoi) sont bonus pour CQL mais enrichissent la vision.
   - Avantage : rigoureux mais accessible, avec beaucoup d'exemples.

3. **Leinster — *Basic Category Theory*** (Cambridge, 2014)
   - Court (190 pages), moderne, très bien écrit.
   - Couvre exactement ce qu'il faut pour CQL : catégories, foncteurs, transformations naturelles, limites/colimites, adjonctions, extensions de Kan.
   - **Gratuit en PDF** sur arXiv : [arxiv.org/abs/1612.09375](https://arxiv.org/abs/1612.09375)

4. **Milewski — *Category Theory for Programmers*** (2019)
   - Pour les développeurs. Exemples en Haskell et C++.
   - Très bon pour développer l'intuition si tu lis du code plus facilement que des maths.
   - **Gratuit** : [github.com/hmemcpy/milewski-ctfp-pdf](https://github.com/hmemcpy/milewski-ctfp-pdf)

### 🟡 Niveau 2 — CQL et les catégories appliquées aux données

C'est le cœur du projet. Ces références relient la théorie des catégories aux bases de données.

5. **Spivak — *Category Theory for the Sciences*** (MIT Press, 2014)
   - David Spivak est l'inventeur de CQL. Ce livre est son introduction accessible.
   - Le **chapitre 3** (Ologs) et le **chapitre 4** (catégories en tant que bases de données) sont directement pertinents.
   - Présente les schemas comme des catégories et les instances comme des foncteurs.
   - **Gratuit en draft** : [math.mit.edu/~dspivak/CT4S.pdf](https://math.mit.edu/~dspivak/CT4S.pdf)

6. **Spivak — *Functorial Data Migration*** (2012)
   - L'article fondateur. Définit Δ, Σ, Π comme des opérations sur les instances.
   - Court (30 pages), très dense, très important.
   - [arxiv.org/abs/1009.1166](https://arxiv.org/abs/1009.1166)

7. **Spivak & Wisnesky — *Relational Foundations for Functorial Data Migration*** (2015)
   - Relie formellement CQL aux bases relationnelles.
   - Montre comment Δ = SELECT/JOIN, Σ = INSERT/UNION, Π = requêtes universelles.
   - [arxiv.org/abs/1212.5303](https://arxiv.org/abs/1212.5303)

8. **Schultz, Spivak & Wisnesky — *Algebraic Databases*** (2017)
   - La formalisation complète : CQL comme théorie algébrique multi-sortée.
   - C'est LA référence technique de ce que Catrust implémente.
   - [arxiv.org/abs/1602.03501](https://arxiv.org/abs/1602.03501)

9. **Site officiel CQL / documentation**
   - [categoricaldata.net](https://categoricaldata.net/)
   - L'IDE Java de référence avec exemples : [github.com/CategoricalData/CQL](https://github.com/CategoricalData/CQL)
   - Tutorial CQL : [categoricaldata.net/help](https://categoricaldata.net/help)

### 🔴 Niveau 3 — Approfondissement mathématique

Pour aller plus loin, ou si tu veux comprendre Π en profondeur.

10. **Mac Lane — *Categories for the Working Mathematician*** (Springer, 1971/1998)
    - La bible. Dense mais complet.
    - Le **chapitre X** sur les extensions de Kan est essentiel pour comprendre Σ et Π comme des adjoints.

11. **Riehl — *Category Theory in Context*** (Dover, 2016)
    - Moderne, exemplifié, plus accessible que Mac Lane.
    - Le chapitre 6 sur les extensions de Kan est excellent.
    - **Gratuit** : [math.jhu.edu/~eriehl/context.pdf](https://math.jhu.edu/~eriehl/context.pdf)

12. **Barr & Wells — *Category Theory for Computing Science*** (1990/1999)
    - Orienté informatique. Sketches, théories algébriques, lien avec les types.
    - **Gratuit** : [tac.mta.ca/tac/reprints/articles/22/tr22abs.html](http://www.tac.mta.ca/tac/reprints/articles/22/tr22abs.html)

13. **Awodey — *Category Theory*** (Oxford, 2010)
    - Très clair, intermédiaire entre Leinster et Mac Lane.
    - Bon sur les adjonctions (chapitre 9) et les topoi (chapitre 12).

### 📐 Niveau bonus — Extensions de Kan et théorie des topoi

Pour quand Π sera implémenté et qu'on voudra aller vers la logique interne.

14. **Johnstone — *Sketches of an Elephant: A Topos Theory Compendium*** (Oxford, 2002)
    - La référence monumentale sur les topoi. Pas pour débuter.
    - Pertinent si Catrust évolue vers un système de types dépendants.

15. **Borceux — *Handbook of Categorical Algebra*** (3 volumes, Cambridge, 1994)
    - Encyclopédie. Le volume 1 couvre les bases, le volume 3 les topoi.

---

## Parcours de lecture recommandé

```
         Lawvere & Schanuel          ← Intuition, zéro prérequis
                │
         Goldblatt (ch.1-6)          ← Rigueur + logique
                │
         Leinster OU Milewski        ← Consolidation (math ou code)
                │
    ┌───────────┴───────────┐
    │                       │
Spivak (CT4S, ch.3-4)    Riehl (ch.1-6)
    │                       │
    └───────────┬───────────┘
                │
  Spivak — Functorial Data Migration  ← L'article fondateur de CQL
                │
  Schultz, Spivak & Wisnesky          ← CQL formel (Algebraic Databases)
                │
          Mac Lane (ch.X)              ← Extensions de Kan (pour Π)
```

---

## Comment lancer

```bash
cargo test    # 27 tests
cargo run     # Démo complète : migration catégorique + génération SQL/Cypher
```

## Licence

MIT
