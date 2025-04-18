import pytest
import pandas as pd

# Fonction pour tester la décomposition du score
import pandas as pd

def test_decomposed_score():
    data = {
        'score': ['6-3 6-4', '7-6 6-3 6-7', '5-7 6-4 6-3', None]
    }
    df = pd.DataFrame(data)

    # Fonction pour décomposer les scores
    def decompose_score(score):
        if score is None:
            return [None, None, None, None, None]
        scores = score.split(' ')
        return scores + [None] * (5 - len(scores))

    # Applique la décomposition à chaque ligne
    df[['set_1', 'set_2', 'set_3', 'set_4', 'set_5']] = df['score'].apply(decompose_score).apply(pd.Series)

    print(df)

test_decomposed_score()

def test_pronostic():
    data = {
        'odd_1': [1.5, 2.0, None, 1.7],
        'odd_2': [2.5, 1.8, 2.0, 1.6],
        'winner': ['player_1', 'player_2', 'player_1', 'player_2']
    }
    df = pd.DataFrame(data)
    
    # Applique la logique de pronostic manuellement ou en utilisant votre logique de transformation DBT
    df['pronostic'] = df.apply(
        lambda row: True if row['odd_1'] < row['odd_2'] and row['winner'] == 'player_1' 
        else True if row['odd_2'] < row['odd_1'] and row['winner'] == 'player_2' 
        else False, axis=1)
    
    # Vérifie  que le pronostic est correctement appliqué
    assert df['pronostic'][0] == True  # Vérifier si le pronostic est correct
    assert df['pronostic'][1] == True  # Vérifier le pronostic pour le deuxième match
    assert df['pronostic'][2] == False # Vérifier un cas où le pronostic est incorrect
    assert df['pronostic'][3] == True  # Vérifier si le pronostic est correct pour un autre cas

def test_translation_surface_and_court():
    data = {
        'surface': ['Clay', 'Grass', 'Hard', 'Carpet', 'Grass'],
        'court': ['Indoor', 'Outdoor', 'Indoor', 'Outdoor', 'Indoor'],
        'round': ['Semifinals', 'Quarterfinals', '1st Round', '2nd Round', '3rd Round']
    }
    df = pd.DataFrame(data)
    
    # Applique la logique de traduction manuellement ou via les transformations DBT
    df['surface_fr'] = df['surface'].map({
        'Clay': 'Terre battue',
        'Grass': 'Gazon',
        'Hard': 'Dur',
        'Carpet': 'Moquette'
    })
    
    df['court_fr'] = df['court'].map({
        'Indoor': 'Intérieur',
        'Outdoor': 'Extérieur'
    })
    
    df['round_fr'] = df['round'].map({
        'Semifinals': 'Demi-finale',
        'Quarterfinals': 'Quart de finale',
        '1st Round': '1er tour',
        '2nd Round': '2e tour',
        '3rd Round': '3e tour'
    })
    
    # Vérifiez que la traduction fonctionne
    assert df['surface_fr'][0] == 'Terre battue'
    assert df['surface_fr'][1] == 'Gazon'
    assert df['court_fr'][2] == 'Intérieur'
    assert df['round_fr'][3] == '2e tour'

def test_dataframe_dimensions():
    data = {
        'tourney_name': ['Australian Open', 'French Open', 'Wimbledon', 'US Open'],
        'score': ['6-3 6-4', '7-6 6-3 6-7', '5-7 6-4 6-3', '6-4 6-3'],
        'date': ['2022-01-01', '2022-02-01', '2023-01-01', '2023-02-01']
    }
    df = pd.DataFrame(data)

    set_columns = df['score'].str.split(' ', expand=True)
    set_columns.columns = [f'set_{i+1}' for i in range(set_columns.shape[1])]
    df = pd.concat([df, set_columns], axis=1)

    for i in range(1, 6):
        set_col = f'set_{i}'
        if set_col not in df.columns:
            df[set_col] = None

    print("Colonnes avant le filtrage:", df.columns)

    # Filtrer les tournois de 2022
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df_2022 = df[df['date'].dt.year == 2022]

    # Vérifier les dimensions après ajout des colonnes de sets
    print("Dimensions de df_2022:", df_2022.shape)

    # Correction de l'assertion : attendre 8 colonnes, pas 6
    assert df_2022.shape == (2, 8)