-- models/transformed/match_transforms.sql

with base as (
    select *
    from {{ ref('stg_matches') }}
),

-- 1️⃣ Décomposer le score en colonnes par set + nombre de sets
decomposed_score as (
    select *,
        split_part(score, ' ', 1) as set_1,
        split_part(score, ' ', 2) as set_2,
        split_part(score, ' ', 3) as set_3,
        split_part(score, ' ', 4) as set_4,
        split_part(score, ' ', 5) as set_5,
        -- Compter le nombre de sets non nuls (si pas de set 4 ou 5 c’est vide)
        (
            case when score is null or score = '' then 0
                 else array_length(string_split(score, ' '), 1)
            end
        ) as nb_sets
    from base
),

-- 2️⃣ Ajout de la colonne "pronostic_ok" (si la plus petite cote gagne)
with_prono as (
    select *,
        case
            when odd_1 is null or odd_2 is null then null
            when odd_1 < odd_2 and winner = player_1 then true
            when odd_2 < odd_1 and winner = player_2 then true
            else false
        end as pronostic_ok
    from decomposed_score
),

-- 3️⃣ Traduction des colonnes court, surface et round
translated as (
    select *,
        -- Surface traduite
        case surface
            when 'Clay' then 'Terre battue'
            when 'Grass' then 'Gazon'
            when 'Hard' then 'Dur'
            when 'Carpet' then 'Moquette'
            else surface
        end as surface_fr,

        -- Court traduit
        case court
            when 'Indoor' then 'Intérieur'
            when 'Outdoor' then 'Extérieur'
            else court
        end as court_fr,

        -- Round traduit
        case round
            when 'Semifinals' then 'Demi-finale'
            when 'Quarterfinals' then 'Quart de finale'
            when '1st Round' then '1er tour'
            when '2nd Round' then '2e tour'
            when '3rd Round' then '3e tour'
            else round
        end as round_fr
    from with_prono
)

-- Résultat final
select *
from translated
