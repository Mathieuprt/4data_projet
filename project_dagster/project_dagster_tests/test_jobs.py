import pandas as pd
from unittest.mock import patch, MagicMock
from dagster import build_asset_context
from project_dagster.assets.atp_asset import atp_asset
import duckdb
from pathlib import Path

def test_atp_asset_partition_execution():
    # Mock DataFrame de retour
    mock_df = pd.DataFrame({
        "Date": ["2023-01-01", "2023-05-06"],
        "Tournament": ["Test A", "Test B"]
    })

    # Patch des fonctions internes et API
    with patch("project_dagster.assets.atp_asset.KaggleApi") as mock_kaggle_api, \
         patch("project_dagster.assets.atp_asset.pd.read_csv") as mock_read_csv, \
         patch("project_dagster.assets.atp_asset.os.listdir") as mock_listdir, \
         patch("project_dagster.assets.atp_asset.reduce_memory_usage", side_effect=lambda x: x), \
         patch("project_dagster.assets.atp_asset.duckdb.connect") as mock_duckdb, \
         patch("project_dagster.assets.atp_asset.Path") as mock_path:

        # Setup des mocks
        mock_api_instance = MagicMock()
        mock_kaggle_api.return_value = mock_api_instance

        mock_listdir.return_value = ["data.csv"]
        mock_read_csv.return_value = mock_df.copy()

        # Mock DuckDB
        mock_conn = MagicMock()
        mock_duckdb.return_value = mock_conn

        # Mock Path pour éviter les accès réels au système de fichiers
        mock_path_obj = MagicMock()
        mock_path.return_value = mock_path_obj
        mock_path_obj.parent.mkdir.return_value = None
        mock_path_obj.__truediv__.return_value = mock_path_obj  # Pour gérer les opérations /

        # Contexte avec partition simulée
        context = build_asset_context(
            partition_key="2023",
            resources={"kaggle_credentials": MagicMock()}
        )

        # Exécution de l'asset
        result = atp_asset(context)

        # Vérifications
        assert isinstance(result.value, pd.DataFrame)
        assert result.metadata["année"].value == "2023"
        assert result.metadata["tournois"].value == 2

        # Vérification des appels DuckDB
        mock_duckdb.assert_called_once()
        mock_conn.execute.assert_any_call("CREATE TABLE IF NOT EXISTS raw_matches AS SELECT * FROM df WHERE 1=0;")
        mock_conn.execute.assert_any_call("INSERT INTO raw_matches SELECT * FROM df")