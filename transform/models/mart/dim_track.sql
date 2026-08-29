
select
    track_id as id,
    track_name as name,
    track_uri as spotify_uri,
    track_number,
    track_duration_ms as duration_ms,
    track_explicit as is_explicit,
    track_disc_number as disc_number

from
    {{ ref("int_recently_played") }}
qualify
    row_number() over (
        partition by track_id order by played_at desc
        ) = 1