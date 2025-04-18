import pandas as pd
from unittest.mock import patch, MagicMock
from dagster import build_op_context, build_asset_context
from project_dagster.assets.atp_asset import atp_asset

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
         patch("project_dagster.assets.atp_asset.reduce_memory_usage", side_effect=lambda x: x):

        # Setup des mocks
        mock_api_instance = MagicMock()
        mock_kaggle_api.return_value = mock_api_instance

        mock_listdir.return_value = ["data.csv"]
        mock_read_csv.return_value = mock_df.copy()

        # Contexte avec partition simulée
        context = build_asset_context(
            partition_key="2023",
            resources={"kaggle_credentials": MagicMock()}
        )

        # Exécution de l'asset
        result = atp_asset(context)

        # Vérifications
        assert isinstance(result.value, pd.DataFrame)
        assert result.metadata["année"].text == "2023"
        assert result.metadata["tournois"].value == 2
