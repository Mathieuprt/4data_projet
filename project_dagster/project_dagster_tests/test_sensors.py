from project_dagster.sensors import new_tournament_sensor
from dagster import RunRequest, SkipReason, build_sensor_context
from dagster import build_sensor_context
import pandas as pd
import os

def test_new_tournament_sensor_detects_new_data(tmp_path):
    # Création d’un fichier CSV avec un tournoi récent
    recent_date = pd.Timestamp.now().strftime("%Y%m%d")
    df = pd.DataFrame({
        "tourney_name": ["ATP Test Open"],
        "tourney_date": [recent_date],
    })
    
    csv_path = tmp_path / "atp_data.csv"
    df.to_csv(csv_path, index=False)

    original_path = "data/atp_data.csv"
    os.makedirs("data", exist_ok=True)
    df.to_csv(original_path, index=False)

    context = build_sensor_context()
    result = new_tournament_sensor(context)

    os.remove(original_path)

    assert isinstance(result, RunRequest)
    assert "new_tournaments" in result.run_key

def test_new_tournament_sensor_skips_when_no_new_data(tmp_path):
    old_date = (pd.Timestamp.now() - pd.Timedelta(days=10)).strftime("%Y%m%d")
    df = pd.DataFrame({
        "tourney_name": ["Old ATP Cup"],
        "tourney_date": [old_date],
    })

    csv_path = tmp_path / "atp_data.csv"
    df.to_csv(csv_path, index=False)

    original_path = "data/atp_data.csv"
    os.makedirs("data", exist_ok=True)
    df.to_csv(original_path, index=False)

    context = build_sensor_context()
    result = new_tournament_sensor(context)

    os.remove(original_path)

    assert isinstance(result, SkipReason)
    assert "Aucun nouveau tournoi détecté" in result.skip_message
