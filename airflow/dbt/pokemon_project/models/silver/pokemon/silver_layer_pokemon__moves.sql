SELECT DISTINCT
    pokemon_id,
    pokemon_name,
    move_name,
    level_learned_at,
    learn_method,
    version_group
FROM {{ source('bronze', 'pokemon_moves') }}
GROUP BY pokemon_id,
    pokemon_name,
    move_name,
    level_learned_at,
    learn_method,
    version_group
ORDER BY pokemon_id, level_learned_at