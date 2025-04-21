import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from dagster import build_op_context
from project_dagster.assets.atp_asset import atp_asset
from kaggle.api.kaggle_api_extended import KaggleApi
import tempfile
import os
from pathlib import Path

@pytest.fixture
def mock_context():
    context = build_op_context(
        resources={"kaggle_credentials": "fake_credentials"},
        partition_key="2022",
    )
    return context

def test_atp_asset(mock_context, monkeypatch):
    # Mock de l'API Kaggle
    mock_kaggle_api = MagicMock(KaggleApi)
    monkeypatch.setattr("project_dagster.assets.atp_asset.KaggleApi", lambda: mock_kaggle_api)

    # Simuler les données
    data = {
        "Date": ["2022-01-01", "2022-02-01", "2023-01-01"],
        "Tournament": ["Open Australia", "French Open", "Wimbledon"],
        "Player": ["Player 1", "Player 2", "Player 3"],
        "Round": ["Final", "Semi-final", "Quarter-final"]
    }
    df = pd.DataFrame(data)

    # Configuration des mocks
    mock_kaggle_api.dataset_download_files = MagicMock()
    mock_kaggle_api.authenticate = MagicMock()

    # Mock de os.listdir et pd.read_csv
    monkeypatch.setattr("project_dagster.assets.atp_asset.os.listdir", lambda x: ["test.csv"])
    monkeypatch.setattr("project_dagster.assets.atp_asset.pd.read_csv", lambda x: df)

    # Mock de DuckDB
    mock_conn = MagicMock()
    monkeypatch.setattr("project_dagster.assets.atp_asset.duckdb.connect", lambda x: mock_conn)

    # Mock tempfile.TemporaryDirectory pour retourner un chemin connu
    temp_dir = "C:/fake_temp_dir"
    monkeypatch.setattr("project_dagster.assets.atp_asset.tempfile.TemporaryDirectory",
                       lambda: MagicMock(__enter__=lambda *args: temp_dir))

    # Appel de la fonction
    result = atp_asset(mock_context)

    # Vérifications des appels à l'API Kaggle
    mock_kaggle_api.authenticate.assert_called_once()
    
    # Vérification avec le chemin temporaire mocké
    mock_kaggle_api.dataset_download_files.assert_called_once_with(
        'dissfya/atp-tennis-2000-2023daily-pull', 
        path=temp_dir,
        unzip=True
    )
    
    # Vérifications sur le DataFrame retourné
    assert len(result.value) == 2  # Doit filtrer pour 2022 seulement
    assert "2022-01-01" in str(result.value['Date'].values)
    assert result.value['Date'].min().year == 2022
    
    # Accéder aux valeurs des métadonnées avec .value
    assert result.metadata["num_rows"].value == 2
    assert result.metadata["année"].value == "2022"
    assert result.metadata["tournois"].value == 2

    # Vérification de l'insertion dans DuckDB
    mock_conn.execute.assert_any_call("CREATE TABLE IF NOT EXISTS raw_matches AS SELECT * FROM df WHERE 1=0;")
    mock_conn.execute.assert_any_call("INSERT INTO raw_matches SELECT * FROM df")