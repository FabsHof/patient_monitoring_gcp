# 🩺 patient monitoring system

> The backend system for monitoring critical patient data, collected via bedside ICU sensors. Includes data like e.g., heart rate, temperature and SPO2 values.

## 🎯 Goals

- Real-Time Dashboarding: Low-latency lookups (e.g., "Show me the last 1 hour of vitals for Patient X").
- Long-Term Analytics: Storing historical data for "Septic Shock" prediction models.

## 📊 Data

Data contains raw vitals data collected from ICU bedside sensors. Each record includes:
- `event_timestamp`
- `sensor_id`
- `heart_rate`
- `body_temperature`
- `spO2`
- `battery_level`

See `notebooks/01_eda.ipynb` for more details on the data and valid cleaning steps.

## 🔨 Setup

- Copy `vitals_raw.txt` to `data/vitals_raw.txt`
- Create a `.env` file based on `.env.example`.
- Install `uv` (see [installation instructions](https://docs.astral.sh/uv/getting-started/installation/))
- Run `uv sync` to install dependencies
- Activate the virtual environment with `source .venv/bin/activate` (MacOS/Linux) or `.venv\Scripts\activate` (Windows)

## 🚀 Commands

> Use `make` to run commands defined in the `Makefile`.

- Ingest and clean data: `make ingest_data`
- Run emulator: `make start_emulator`
- Load cleaned data into Bigtable: `make load_bigtable`
