# 💸 NSIA Vie — Portail Suivi des Impayés

Application Streamlit de suivi et d'analyse des quittances impayées (branche Vie Individuel), connectée à la base **SUN_COTEDIVOIRE** via SQL Server.

---

## 🗂️ Fonctionnement général

L'application adopte une architecture **locale-first** en deux temps :

1. **Extraction initiale** — interroge la base SQL Server par semestres (de 2015 à aujourd'hui) et sauvegarde le résultat dans un fichier `NSIA_Impayes_BASE.parquet` (format compact, chargement < 2 s même pour 1 M de lignes).
2. **Utilisation quotidienne** — tous les filtres s'appliquent **en mémoire** sur le Parquet local, sans aucun appel SQL → réponse instantanée.
3. **Mise à jour incrémentale** — le bouton *"🔄 Mettre à jour"* extrait uniquement les données depuis la dernière date connue et les fusionne dans le Parquet existant.

---

## ✅ Règles métier figées

| Règle | Détail |
|---|---|
| Périmètre | **INDIVIDUEL uniquement** (`JAQUITP → JAPOLIP → JAIDENP`) |
| Statut encaissement | `INENC = ' '` (non encaissé) |
| Polices exclues | Toutes les polices dont le numéro **commence par 8** |
| Produits exclus | `5100 5200 5300 6100 6120 6400 6420 6625 7520 7525 7550` |

---

## 🚀 Installation

### Prérequis
- Python **3.10+**
- Pilote ODBC **"ODBC Driver 17 for SQL Server"** installé sur la machine
- Accès réseau au serveur `10.8.3.9\SUNCOTEDIVOIRE`

### Étapes

```bash
# 1. Cloner le dépôt
git clone https://github.com/<votre-org>/nsia-vie-impayes.git
cd nsia-vie-impayes

# 2. Créer un environnement virtuel (recommandé)
python -m venv .venv
source .venv/bin/activate       # Linux / macOS
.venv\Scripts\activate          # Windows

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Lancer l'application
streamlit run app.py
```

L'application s'ouvre automatiquement sur `http://localhost:8501`.

---

## 📥 Premier lancement

Lors du premier démarrage, **aucune base locale n'existe**. L'application affiche le panneau d'extraction.

1. Choisissez le **mode de sélection de la période** :
   - *Depuis une année* — recommandé pour une extraction ciblée
   - *Période personnalisée* — pour un intervalle précis
   - *Tout depuis 2015* — extraction complète (15–45 min selon le réseau)
2. Cliquez sur **📥 Télécharger / Remplacer la base**.
3. L'extraction se fait par **semestres successifs** avec une barre de progression.
4. Une fois terminée, l'application se recharge automatiquement.

---

## 🔄 Mise à jour des données

Cliquez sur **🔄 Mettre à jour** dans le panneau d'extraction. L'application détecte automatiquement la dernière date présente dans le Parquet et n'extrait que les nouvelles données.

---

## 🎛️ Filtres disponibles

### Sidebar
| Filtre | Description |
|---|---|
| Colonne de date | Choisir sur quelle date porte le filtre (début période, date quittance, date effet, etc.) |
| Période | Plage de dates sur la colonne sélectionnée |
| Agents | Filtrer par code(s) agent (un par ligne) |
| Date d'affectation | Filtre indépendant sur la date d'affectation bancaire (JAENCAP) |

### Filtres additionnels
Mode d'encaissement · Motif de prélèvement · Classe durée impayé · Ancienneté police · Code produit · Statut police · Périodicité · Genre · Recherche libre (N° Police / Nom / Prénom).

### Vues rapides
Raccourcis en un clic : *Provision insuffisante*, *Compte clos*, *Compte inexistant*, *Mobile / Numérique*, *≥ 12 impayés*, *Montant > 500 000 F*.

---

## 📊 Onglets

| Onglet | Contenu |
|---|---|
| 📋 Extraction | Tableau détaillé des quittances avec sélection de colonnes et export Excel / CSV |
| 🏦 Mode / Motif | Répartition par mode d'encaissement, motif de rejet et top 20 agents |
| 📦 Produit / Statut | Analyse par code produit, statut police, périodicité et nombre d'impayés |
| ⏱ Durée / Ancienneté | Classes de durée d'impayé et d'ancienneté des polices |
| 👤 Profil client | Répartition par genre et tranche d'âge à la souscription |
| 📈 Évolution | Tendances mensuelles et annuelles |

---

## 📤 Export

- **CSV** : disponible immédiatement, encodage UTF-8 BOM (compatible Excel).
- **Excel (.xlsx)** : généré à la demande (bouton *"Préparer Excel"*), avec en-tête coloré NSIA, largeur de colonnes automatique et volet figé.
- La sélection des colonnes à exporter est entièrement personnalisable par groupe (Client, Police, Quittance, Montants, Mode/Motif, Banque).

---

## 🗃️ Fichiers générés localement

| Fichier | Description |
|---|---|
| `NSIA_Impayes_BASE.parquet` | Base de données locale (format Parquet/Snappy) |
| `NSIA_Impayes_META.json` | Métadonnées : nb lignes, date MAJ, plage couverte |

> Ces fichiers sont créés dans le **même répertoire que `app.py`**. Ils ne doivent pas être commités dans Git (voir `.gitignore`).

---

## 🔒 Sécurité

> ⚠️ Les identifiants de connexion SQL Server sont actuellement écrits en dur dans le code (`UID=reportdata / PWD=reportdata$2025`). Pour un usage en production ou une publication sur un dépôt public, il est **fortement recommandé** de les externaliser via des variables d'environnement ou le mécanisme de **Secrets Streamlit** (`st.secrets`).

Exemple avec `secrets.toml` :

```toml
# .streamlit/secrets.toml  (ne pas commiter ce fichier)
[db]
SERVER   = "10.8.3.9\\SUNCOTEDIVOIRE"
DATABASE = "SUN_COTEDIVOIRE"
UID      = "reportdata"
PWD      = "reportdata$2025"
```

---

## 📦 Dépendances principales

| Package | Rôle |
|---|---|
| `streamlit` | Framework UI |
| `pandas` | Manipulation des données |
| `numpy` | Calculs numériques |
| `pyodbc` | Connexion SQL Server |
| `pyarrow` | Lecture/écriture Parquet |
| `openpyxl` | Export Excel |
| `python-dateutil` | Calcul des semestres |

---

## 🏢 Contexte

Développé pour la **Direction des Études Réassurance et Actuariat** de **NSIA Vie Assurances — Côte d'Ivoire**.

---

## .gitignore recommandé

```
# Données locales (à ne jamais commiter)
NSIA_Impayes_BASE.parquet
NSIA_Impayes_META.json

# Secrets Streamlit
.streamlit/secrets.toml

# Python
__pycache__/
*.pyc
.venv/
*.egg-info/
dist/
build/
```
