import pytest
from unittest.mock import MagicMock
import pandas as pd
from dagster import build_op_context
from project_dagster.assets.atp_asset import atp_asset 

# Test de l'asset atp_asset
@pytest.fixture
def mock_context():
    # Contexte pour simulé avec la partition (année)
    context = build_op_context(
        resources={"kaggle_credentials": "fake_credentials"},
        partition_key="2022",  # Année de partition
    )
    return context

def test_atp_asset(mock_context):
    # Une petite version simulée du CSV
    data = {
        "Date": ["2022-01-01", "2022-02-01", "2023-01-01"],
        "Tournament": ["Open Australia", "French Open", "Wimbledon"],
        "Player": ["Player 1", "Player 2", "Player 3"],
        "Round": ["Final", "Semi-final", "Quarter-final"]
    }
    df = pd.DataFrame(data)

    # Simulation du comportement de la lecture du fichier CSV (au lieu de télécharger et lire depuis Kaggle)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(pd, "read_csv", lambda x: df)

        # Appel de l'asset atp_asset avec le contexte simulé
        result = atp_asset(mock_context)

        # Vérifie que l'output est bien ce que l'on attend
        assert result.value.shape == (2, 4)
        assert "num_rows" in result.metadata
        assert result.metadata["num_rows"].value == 2 
        assert "période" in result.metadata
        assert result.metadata["période"].text == "2022-01-01 to 2022-02-01" 
