var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/google";
var report_url = "https://content.googleapis.com/webmasters/v3/sites/%s/searchAnalytics/query?fields=rows&alt=json";
var mainProperties = ["clicks", "impressions", "ctr", "position"];

var fetch = function (parameters, events, startDate, endDate) {
    logger.debug("Fetching between " + startDate + " and " + (endDate || 'now'));
    if (endDate == null) {
        endDate = new Date();
        endDate.setDate(endDate.getDate() - 1);
        endDate = endDate.toJSON().slice(0, 10);
    }

    startDate = startDate || config.get('start_date');

    if (startDate == null) {
        startDate = new Date();
        startDate.setMonth(startDate.getMonth() - 5);
        startDate = startDate.toJSON().slice(0, 10);
    }

    if (startDate === endDate) {
        logger.info("No data to process");
        return;
    }

    var response = http.get(oauth_url)
        .query('refresh_token', parameters.refresh_token)
        .send();
    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var data = response.getResponseBody();
    response = http.post(report_url.replace('%s', encodeURIComponent(parameters.website_url)))
        .header('Authorization', data)
        .header('Content-Type', "application/json")
        .data(JSON.stringify({
            dimensions: ["date", "country", "device", "page", "query"],
            responseAggregationType: "byPage",
            rowLimit: 5000,
            startDate: startDate, endDate: endDate
        }))
        .send();

    if (response.getStatusCode() != 200) {
        if(response.getStatusCode() == 404) {
            throw new Error("The website "+parameters.website_url+" could not found");
        }

        throw new Error(response.getResponseBody())
    }

    var data = JSON.parse(response.getResponseBody());
    var events = data.rows.map(function (row) {
        var properties = {
            '_time': row.keys[0] + "T00:00:00",
            country: row.keys[1],
            device: row.keys[2],
            page: row.keys[3],
            query: row.keys[4]
        }
        mainProperties.forEach(function(term) {
            properties[term] = row[term];
        });

        return {collection: parameters.collection, properties: properties};
    });

    eventStore.store(events);
    config.set('start_date', endDate);
}

var main = function (parameters) {
    return fetch(parameters, [])
}