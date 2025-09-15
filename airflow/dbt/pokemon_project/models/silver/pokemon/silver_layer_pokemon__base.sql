SELECT
    id as pokemon_id,
    name as pokemon_name,
    type_1,
    coalesce(type_2, 'none') as type_2,
    weight as pokemon_weight,
    height as pokemon_height,
    sprite_url
FROM {{ source('bronze', 'pokemon_base') }}
ORDER BY pokemon_id