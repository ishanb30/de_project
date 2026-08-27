
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="played_at"
    )
}}

with new_rows as (
    select
        raw_data:played_at::timestamp_tz as played_at,
        raw_data:track:id::varchar as track_id,
        raw_data:track:album as track_album,
        raw_data:track:artists as track_artists,
        raw_data:track:disc_number::int as track_disc_number,
        raw_data:track:duration_ms::int as track_duration_ms,
        raw_data:track:explicit::boolean as track_explicit,
        raw_data:track:name::varchar as track_name,
        raw_data:track:track_number::int as track_number,
        raw_data:track:uri::varchar as track_uri,
        raw_data:context:type::varchar(15) as context_type,
        loaded_at

    from
        {{ source("spotify_api", "RECENTLY_PLAYED") }}

    {% if is_incremental() %}
    where
        played_at > (select max(played_at) - interval '{{ var('lookback_window_mins') }} minutes' from {{ this }})
    {% endif %}
)
, deduplication as (
    select
        *,
        row_number() over (partition by played_at order by loaded_at desc) as rn
    from
        new_rows
)

select
    played_at,
    track_id,
    track_album,
    track_artists,
    track_disc_number,
    track_duration_ms,
    track_explicit,
    track_name,
    track_number,
    track_uri,
    context_type

from
    deduplication
where
    rn = 1