
select
    artist.value:id::varchar as id,
    artist.value:name::varchar as name,
    artist.value:uri::varchar as spotify_uri

from
    {{ ref("int_recently_played") }},
        lateral flatten (input => track_artists) as artist
qualify
    row_number() over (
        partition by id order by played_at desc
    ) = 1