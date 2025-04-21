import pytest
from unittest.mock import MagicMock
import subprocess
from dagster import build_op_context
from project_dagster.assets.dbt_asset import dbt_run

# Test unitaire pour l'asset dbt_run
def test_dbt_run_success():
    # Contexte simulé pour le test
    mock_context = build_op_context(resources={})

    # Simule le comportement de subprocess.run pour un succès
    with pytest.MonkeyPatch.context() as mp:
        # Remplace subprocess.run par un mock qui simule une sortie réussie
        mock_subprocess = MagicMock()
        mock_subprocess.return_value = subprocess.CompletedProcess(
            args=['dbt', 'run'],
            returncode=0,
            stdout="DBT run completed successfully",
            stderr=""
        )
        mp.setattr(subprocess, 'run', mock_subprocess)

        result = dbt_run(mock_context)

        # Vérifie que subprocess.run a été appelé avec les bons arguments
        mock_subprocess.assert_called_once_with(
            ['dbt', 'run'],
            capture_output=True,
            text=True,
            cwd='../projet_dbt'
        )

        # Vérifie que la sortie de l'asset est correcte
        assert result == "DBT run completed successfully"


def test_dbt_run_failure():
    # Contexte simulé pour le test
    mock_context = build_op_context(resources={})

    # Simule le comportement de subprocess.run pour un échec
    with pytest.MonkeyPatch.context() as mp:
        # Remplace subprocess.run par un mock qui simule une sortie échouée
        mock_subprocess = MagicMock()
        mock_subprocess.return_value = subprocess.CompletedProcess(
            args=['dbt', 'run'],
            returncode=1,
            stdout="",
            stderr="DBT run failed"
        )
        mp.setattr(subprocess, 'run', mock_subprocess)

        # Test de l'échec
        with pytest.raises(Exception) as exc_info:
            dbt_run(mock_context)

        # Vérifie que l'exception contient l'erreur attendue
        assert "DBT run failed with error code 1" in str(exc_info.value)
        mock_subprocess.assert_called_once_with(
            ['dbt', 'run'],
            capture_output=True,
            text=True,
            cwd='../projet_dbt'
        )
