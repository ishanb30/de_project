
{% docs play_event_key %}
Composite key constructed with played_at and track_id. This is the 
grain of the play-event models
{% enddocs %}

{% docs played_at %}
The UTC timestamp at which the track finished playing. Sourced from
Spotify's `played_at` field. 
{% enddocs %}


{% docs track_id %}
Spotify's unique identifier for a track. The same recording can have 
different IDs across releases (e.g. as a single vs. on an album)
{% enddocs %}


{% docs album_id %}
Spotify's unique identifier for an album.
{% enddocs %}


{% docs artist_id %}
Spotify's unique identifier for an artist.
{% enddocs %}