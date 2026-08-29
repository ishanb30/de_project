
select
    album_id as id,
    album_name as name,
    album_type as type,
    album_total_tracks as total_tracks,
    album_release_date as release_date,
    album_release_date_precision as release_date_precision,
    album_uri as spotify_uri

from
    {{ ref("int_recently_played") }}
qualify
    row_number() over(
        partition by album_id order by played_at desc
    ) = 1