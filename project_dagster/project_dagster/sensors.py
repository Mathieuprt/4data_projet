# sensors/new_tournament_sensor.py
from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext
import pandas as pd
from datetime import datetime
from project_dagster.jobs import atp_elt_job

@sensor(job=atp_elt_job)
def new_tournament_sensor(context: SensorEvaluationContext):
    # Détecte les nouveaux tournois ATP dans les données
    try:
        df = pd.read_csv('data/atp_data.csv')
        
        # Trouver les tournois récents (derniers 7 jours)
        df['tourney_date'] = pd.to_datetime(df['tourney_date'], format='%Y%m%d')
        recent_tournaments = df[df['tourney_date'] > datetime.now() - pd.Timedelta(days=7)]
        
        if not recent_tournaments.empty:
            new_tournaments = recent_tournaments['tourney_name'].unique()
            context.log.info(f"Nouveaux tournois détectés: {new_tournaments}")
            return RunRequest(run_key=f"new_tournaments_{datetime.now().timestamp()}")
        
        return SkipReason("Aucun nouveau tournoi détecté")
    
    except Exception as e:
        context.log.error(f"Erreur dans le sensor: {str(e)}")
        return SkipReason(f"Erreur de traitement: {str(e)}")