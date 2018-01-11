//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/mixpanel/script.js

var report_url = "data.mixpanel.com/api/2.0/export/";

var fetch = function (parameters, events, index, startDate, endDate) {
    logger.debug("Fetching between " + startDate + " and " + (endDate || 'now') + (index == null ? "" : " for index" + index));
    if (endDate == null) {
        endDate = new Date();
        endDate.setDate(endDate.getDate() - 1);
        endDate = endDate.toJSON().slice(0, 10);
    }

    startDate = startDate || config.get('start_date' + (index == null ? "" : "." + index));

    if (startDate == null) {
        startDate = new Date();
        startDate.setDate(startDate.getDate() - 2);
        startDate = startDate.toJSON().slice(0, 10);
    }

    if (startDate === endDate) {
        logger.info("No data to process");
        return;
    }

    var endGap = new Date(endDate);
    endGap.setDate(endGap.getDate() - 1);
    var response = http.get("https://" + report_url)
        .header('Authorization', 'Basic ' + util.base64.encode(parameters.api_secret))
        .query('from_date', startDate)
        .query('to_date', endGap.toJSON().slice(0, 10))
        .send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var data = response.getResponseBody().split(/\n/);

    var mapping = {
        "$browser": "_user_agent_family",
        "$browser_version": "_user_agent_version",
        "$current_url": "_url"
    }

    var utcOffset = parameters.timezone * 60 * 1000;
    var events = [];
    for (var i = 0; i < data.length; i++) {
        try {
            var row = JSON.parse(data[i]);
        } catch (e) {
            logger.warn(data[i]);
            continue;
        }
        row.collection = row.event;
        row.event = undefined;

        var properties = row.properties;
        properties._time = (properties.time * 1000) + utcOffset;
        properties.time = undefined;

        properties['$lib_version'] = undefined;

        for (var key in mapping) {
            properties[mapping[key]] = properties[key];
            properties[key] = undefined;
        }

        for (var key in properties) {
            if (key[0] == '$') {
                var newKey = "_" + key.substring(1);
                properties[newKey] = properties[key];
                properties[key] = undefined;
            }
        }
        events.push(row);
    }

    eventStore.store(events);
    config.set('start_date', endDate);
}

var main = function (parameters) {
    return fetch(parameters, []);
}