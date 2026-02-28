# ⚽ ValueBet Bot

Détection automatique de paris à valeur en **Ligue 1** et **Premier League** via modèle de Poisson et Expected Goals.

## Architecture

```
valuebet/
├── app.py              # Flask web interface
├── scheduler.py        # Daily job engine (APScheduler)
├── model.py            # Poisson prediction model
├── api_clients.py      # The Odds API + API-Sports wrappers
├── database.py         # SQLite persistence layer
├── telegram_bot.py     # Telegram notifications
├── templates/          # HTML templates (Jinja2)
│   ├── base.html
│   ├── index.html
│   ├── history.html
│   ├── stats.html
│   └── live.html
├── requirements.txt
├── Procfile            # Railway deployment
├── railway.toml
└── .env.example
```

## Installation locale

```bash
# 1. Clone and install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys

# 3. Initialize database
python -c "from database import init_db; init_db()"

# 4. Load team stats (do this first!)
python scheduler.py refresh

# 5. Run value bet engine manually
python scheduler.py run

# 6. Start web interface
python app.py
# → http://localhost:5000
```

## Commandes scheduler

```bash
python scheduler.py run       # Exécution immédiate (value bets)
python scheduler.py refresh   # Mise à jour stats équipes
python scheduler.py schedule  # Démarrer le cron (bloquant)
```

## Déploiement Railway

### 1. Préparer le projet

```bash
git init
git add .
git commit -m "Initial commit"
```

### 2. Créer les services Railway

1. Connectez-vous sur [railway.app](https://railway.app)
2. Créez un nouveau projet → **Deploy from GitHub**
3. Liez votre repository

### 3. Variables d'environnement (Railway → Variables)

```
ODDS_API_KEY=your_key
APISPORTS_KEY=your_key
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
VALUE_THRESHOLD=0.05
MIN_PROBABILITY=0.55
TOP_BETS_COUNT=5
SEASON=2024
LEAGUES=61,39
SCHEDULER_HOUR=8
DB_PATH=/data/valuebet.db
```

> **Important :** Pour persister la base SQLite sur Railway, créez un **Volume** Railway et montez-le sur `/data`.

### 4. Ajouter un service worker

Dans Railway, ajoutez un second service avec la commande :
```
python scheduler.py schedule
```

### 5. Obtenir votre Telegram Chat ID

```
1. Créez un bot via @BotFather → notez le token
2. Envoyez un message à votre bot
3. Visitez: https://api.telegram.org/bot<TOKEN>/getUpdates
4. Trouvez "chat":{"id": XXXXXX} → c'est votre CHAT_ID
```

## Modèle de prédiction

### Forces attaque/défense

```
att_force_home = (buts marqués à domicile / matchs) / moyenne ligue domicile
def_force_home = (buts encaissés à domicile / matchs) / moyenne ligue extérieur

lambda_home = att_force_home × def_force_away × moyenne_buts_domicile_ligue
lambda_away = att_force_away × def_force_home  × moyenne_buts_extérieur_ligue
```

### Distribution de Poisson

```
P(X=k) = e^(-λ) × λ^k / k!

Matrice scores P(home=i, away=j) = P_home(i) × P_away(j)
→ 1X2, Over/Under 2.5, BTTS
```

### Détection de value

```
Value = (Cote_BK × Probabilité_modèle) - 1

Si Value > VALUE_THRESHOLD (défaut 5%) → value bet !
```

## Limites et améliorations futures

- Ajouter le suivi `fixture_id` dans la table `bets` pour auto-update des résultats
- Intégrer des données xG réelles (StatsBomb open data)
- Modèle Dixon-Coles (correction sur les scores 0-0 et 1-1)
- Marchés supplémentaires : BTTS, Over/Under depuis odds API
- Backtesting sur données historiques

## ⚠️ Disclaimer

Ce bot est un outil d'analyse statistique à titre éducatif. Les paris sportifs comportent des risques. Pariez de façon responsable.
