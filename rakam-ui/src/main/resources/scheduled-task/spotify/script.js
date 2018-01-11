//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/spotify/script.js

var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/spotify";
var report_url = "https://api.spotify.com/v1/me/player/recently-played";

var fetchItems = function (arr, access_token, before, collection) {
    var response = http.get(report_url + '?limit=50&before=' + (before || (new Date().getTime())))
        .header("Authorization", "Bearer " + access_token).send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var body = JSON.parse(response.getResponseBody());

    var max = 0;

    body.items.forEach(function (item) {
        var song = {};
        song._time = item.played_at;
        max = Math.max(new Date(item.played_at).getTime(), max);
        song.track_duration = item.track.duration_ms;
        song.track_id = item.track.id;
        song.album_id = item.track.album.id;
        song.album_name = item.track.album.name;
        song.disc_number = item.track.disc_number;
        song.track_name = item.track.name;
        song.popularity = item.track.popularity;
        song.track_number = item.track.track_number;
        song.artist_names = item.track.artists.map(function (artist) {
            return artist.name;
        });
        song.artist_ids = item.track.artists.map(function (artist) {
            return artist.id;
        });

        if (item.track.external_ids) {
            Object.keys(item.track.external_ids).forEach(function (key) {
                song['external_id_' + key] = item.track.external_ids[key];
            })
        }
        arr.push({collection: collection, properties: song});
    });

    // if (body.cursors && body.cursors.after) {
    //     return fetchItems(arr, access_token, body.cursors.after, collection);
    // }

    return max;
}

var fetch = function (parameters, offset) {
    logger.debug("Fetching with offset " + offset);

    offset = offset || config.get('offset');

    var response = http.get(oauth_url + "?refresh_token=" + parameters.refresh_token)
        .header('Authorization', "Bearer " + parameters.access_token)
        .send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var access_token = JSON.parse(response.getResponseBody()).access_token;

    var arr = [];
    var endDate = fetchItems(arr, access_token, offset, parameters.collection);

    eventStore.store(arr);
    config.set('offset', endDate);
}

var main = function (parameters) {
    return fetch(parameters)
}