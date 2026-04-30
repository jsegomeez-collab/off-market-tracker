# OFFMARKET — Deal Radar NEPA

Sistema automatizado para detectar oportunidades inmobiliarias **off-market** en Luzerne County (Sprint 1).
Cero costo recurrente: corre en GitHub Actions, datos en SQLite committed al repo, scoring por reglas.

## Qué hace

Cada 6 horas:
1. **Scrapea** fuentes públicas off-market (tax repository, sheriff sales, FSBO Craigslist).
2. **Deduplica** por `(source, source_id)` y persiste en `data/deals.db`.
3. **Puntúa** cada propiedad nueva (0-100) según precio, fuente, ciudad y señales de distress.
4. **Notifica** los deals con score ≥ 40 vía Telegram + email Gmail.

## Fuentes activas (Sprint 1)

| Fuente | Tipo | Estado |
|---|---|---|
| Luzerne County Tax Claim - Repository List (PDF) | Lista de propiedades unsold en judicial sale, comprables por bid mínimo $500-$1,000 | activo |
| Luzerne County Sheriff Sales | Foreclosures bancarias | activo (sitio flaky) |
| Craigslist Scranton FSBO | Listings "by owner" filtrados a municipalidades de Luzerne | activo |

## Setup en GitHub Actions

1. Push del repo a GitHub (privado recomendado).
2. Configurar secrets en Settings → Secrets and variables → Actions:

   | Secret | Valor |
   |---|---|
   | `TELEGRAM_BOT_TOKEN` | Crear bot con [@BotFather](https://t.me/BotFather) |
   | `TELEGRAM_CHAT_ID` | Tu chat ID — habla con [@userinfobot](https://t.me/userinfobot) |
   | `GMAIL_USER` | tu email gmail |
   | `GMAIL_APP_PASSWORD` | [App password](https://myaccount.google.com/apppasswords) (no tu password normal) |
   | `NOTIFY_EMAIL` | email destino (puede ser el mismo que `GMAIL_USER`) |

3. El workflow corre cada 6h automáticamente. Para correr manualmente: Actions → "Deal Radar" → Run workflow.

Si no configuras Telegram o Gmail, ese canal se omite silenciosamente — el resto sigue funcionando.

## Setup local

```bash
pip install -r requirements.txt
python -m src.main
```

Sin secrets configurados, los scrapers corren y la DB se popula, pero no se envían notificaciones.

## Estructura

```
src/
├── models.py              # Property dataclass
├── db.py                  # SQLite schema + helpers
├── scoring.py             # Reglas de scoring
├── notify.py              # Telegram + Gmail SMTP
├── main.py                # Orchestrator
└── scrapers/
    ├── luzerne_tax_repo.py
    ├── luzerne_sheriff.py
    └── craigslist_scranton.py
data/
└── deals.db               # SQLite, committed para historial cross-runs
.github/workflows/scrape.yml
```

## Scoring (Sprint 1, reglas duras)

Hard filter: si `county != 'luzerne'` → descarta.

Suma:
- **Source base:** tax_repo +35, sheriff +20, craigslist +10
- **Price tier:** ≤$5k +30, ≤$30k +20, ≤$70k +10, >$300k -10
- **Distress keywords** (estate, as-is, motivated, vacant, etc.): +5 c/u, máx +20
- **Multi-unit** (duplex, triplex, multifamily): +15
- **Top city** (Wilkes-Barre, Hazleton, Nanticoke, Kingston, Pittston): +10

Threshold de notificación: **score ≥ 40** (configurable vía env `MIN_NOTIFY_SCORE`).

## Roadmap

- **Sprint 2:** PA Bulletin RSS, Recorder of Deeds (heredades), TruePeopleSearch skip-trace, Gemini Flash scoring (gratis).
- **Sprint 3:** Facebook Marketplace + 5 grupos FB (Playwright), RTKL automation para code violations + delinquent water/sewer, dashboard web.
- **Expansión counties:** Lackawanna, Schuylkill, Northumberland, Carbon, Lehigh, Columbia, Monroe.

## Inspeccionar la DB

```bash
sqlite3 data/deals.db "SELECT score, source, listing_price, city, address FROM properties ORDER BY score DESC LIMIT 20;"
```
