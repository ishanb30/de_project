
select
    play_event_key,
    played_at,
    track_id,
    track_artists,
    track_disc_number,
    track_duration_ms,
    track_explicit,
    track_name,
    track_number,
    track_uri,
    context_type,
    track_album:album_type::varchar(11) as album_type,
    track_album:total_tracks::int as album_total_tracks,
    track_album:id::varchar as album_id,
    track_album:name::varchar as album_name,
    track_album:release_date::varchar(10) as album_release_date,
    track_album:release_date_precision::varchar(5) as album_release_date_precision,
    track_album:uri::varchar as album_uri,
    track_album:artists as album_artists

from {{ ref("stg_recently_played") }}
