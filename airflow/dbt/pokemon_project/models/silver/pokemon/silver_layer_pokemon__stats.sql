SELECT
    id as pokemon_id,
    name as pokemon_name,
    hp,
    speed,
    attack,
    special_attack,
    defense,
    special_defense,
    base_experience
FROM {{ source('bronze', 'pokemon_base') }}
ORDER BY pokemon_id