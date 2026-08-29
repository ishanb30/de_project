
select
    play_event_key,
    played_at,
    album_id,
    track_id,
    context_type as playback_source

from {{ ref("int_recently_played") }}