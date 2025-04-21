# project_dagster_tests/test_sensors.py
from project_dagster.sensors import new_tournament_sensor
from dagster import RunRequest, SkipReason, build_sensor_context
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
import duckdb

def test_new_tournament_sensor_detects_new_data():
    recent_date = datetime.now(timezone.utc) - timedelta(days=1)

    with patch('project_dagster.sensors.KaggleApi') as mock_kaggle_api:
        mock_api = MagicMock()

        # Setup dataset mock with required attributes
        mock_dataset = MagicMock()
        mock_dataset.ref = "dissfya/atp-tennis-2000-2023daily-pull"
        mock_dataset.lastUpdated = recent_date

        mock_api.dataset_list.return_value = [mock_dataset]
        mock_kaggle_api.return_value = mock_api

        with patch('project_dagster.sensors.duckdb.connect') as mock_duckdb:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetchone.return_value = [
                recent_date - timedelta(days=2)
            ]
            mock_duckdb.return_value = mock_conn

            context = build_sensor_context()
            result = new_tournament_sensor(context)

            assert isinstance(result, RunRequest)
            assert "kaggle_update" in result.run_key


def test_new_tournament_sensor_skips_when_no_new_data():
    old_date = datetime.now(timezone.utc) - timedelta(days=10)

    with patch('project_dagster.sensors.KaggleApi') as mock_kaggle_api, \
         patch('project_dagster.sensors.Path.exists') as mock_exists, \
         patch('project_dagster.sensors.duckdb.connect') as mock_duckdb:

        mock_api = MagicMock()

        mock_dataset = MagicMock()
        mock_dataset.ref = "dissfya/atp-tennis-2000-2023daily-pull"
        mock_dataset.lastUpdated = old_date

        mock_api.dataset_list.return_value = [mock_dataset]
        mock_kaggle_api.return_value = mock_api

        mock_exists.return_value = True

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = [datetime.now(timezone.utc)]
        mock_duckdb.return_value = mock_conn

        context = build_sensor_context()
        result = new_tournament_sensor(context)

        assert isinstance(result, SkipReason)
        assert "No new data on Kaggle" in result.skip_message
