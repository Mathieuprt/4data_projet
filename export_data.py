import duckdb
import os

# Chemin vers ta base (à rendre dynamique)
db_path = r'C:\Users\mathi\Desktop\SUPINFO\4DATA\4data_projet\projet_dbt\atp_tennis.duckdb'

# Dossier d'export (crée s'il n'existe pas)
export_dir = r'C:\Users\mathi\Desktop\SUPINFO\4DATA\exports\4data_projet'
os.makedirs(export_dir, exist_ok=True)

# Connexion à DuckDB
conn = duckdb.connect(db_path)

# Récupérer toutes les tables de la base
tables = conn.execute("SHOW TABLES").fetchall()

for (table_name,) in tables:
    export_path = os.path.join(export_dir, f"{table_name}.csv")
    print(f"📤 Export de la table {table_name} vers {export_path}")
    
    conn.execute(f"COPY {table_name} TO '{export_path}' (HEADER, DELIMITER ',');")

conn.close()
print("✅ Tous les exports sont terminés.")
