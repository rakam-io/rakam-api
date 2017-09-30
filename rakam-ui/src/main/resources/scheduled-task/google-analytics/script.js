//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/google-analytics/script.js

var ONE_WEEK = 1000 * 60 * 60 * 24 * 7;
var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/google";
var report_url = "https://analyticsreporting.googleapis.com/v4/reports:batchGet";

var fetch = function (parameters, startDate, endDate, nextToken, accessKey) {
    logger.debug("Fetching between " + startDate + " and " + (endDate || 'now') + (nextToken ? (' with token ' + nextToken) : ''));
    if (endDate == null) {
        var token = config.get('cursor');
        if (token != null) {
            var parsed = token.split(' ');
            startDate = parsed[0];
            nextToken = parsed[1];
        }
    }

    if (startDate == null) {
        startDate = new Date();
        startDate.setMonth(startDate.getMonth() - 12);
        startDate = startDate.toJSON().slice(0, 10);
    }

    if (endDate == null) {
        endDate = new Date();
        endDate.setDate(endDate.getDate() - 1);

        var tempStartDate = new Date(startDate).getTime();
        if (endDate.getTime() - tempStartDate > ONE_WEEK) {
            endDate = new Date(tempStartDate + ONE_WEEK);
        }

        endDate = endDate.toJSON().slice(0, 10);
    }

    if (startDate === endDate) {
        logger.info("No data to process");
        return;
    }

    if (accessKey == null) {
        var response = http.get(oauth_url)
            .query('refresh_token', parameters.refresh_token)
            .send();

        if (response.getStatusCode() == 0) {
            throw new Error(response.getResponseBody());
        }

        if (response.getStatusCode() != 200) {
            throw new Error(response.getResponseBody());
        }

        accessKey = response.getResponseBody();
    }

    var metrics = parameters.metrics.split(',');
    var dimensions = parameters.dimensions.split(',');
    response = http.post(report_url)
        .header('Authorization', accessKey)
        .data(JSON.stringify({
            reportRequests: [
                {
                    viewId: parameters.profile_id,
                    pageToken: nextToken,
                    pageSize: 10000,
                    dateRanges: [{"startDate": startDate, "endDate": endDate}],
                    metrics: metrics.map(function (metric) {
                        return {"expression": metric}
                    }),
                    dimensions: dimensions.map(function (dim) {
                        return {"name": dim}
                    }),
                    segments: parameters.segment ? [{"segmentId": parameters.segment_id}] : undefined,
                    dimensionFilterClauses: parameters.dimension_filter ? [
                        {
                            filters: [
                                {
                                    dimensionName: parameters.dimension_filter.split(' ', 3)[0],
                                    operator: parameters.dimension_filter.split(' ', 3)[1],
                                    expressions: JSON.parse(parameters.dimension_filter.split(' ', 3)[2])
                                }
                            ]
                        }
                    ] : undefined
                }
            ]
        }))
        .send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var data = JSON.parse(response.getResponseBody());

    var metricMappers = data.reports[0].columnHeader
        .metricHeader.metricHeaderEntries.map(function (item) {
            if (item.type == 'INTEGER') {
                return parseInt;
            }
            else if (item.type == 'CURRENCY') {
                return parseFloat;
            }
            else {
                return function (a) {
                    return a
                };
            }
        });

    var events = [];
    var report = data.reports[0];
    (report.data.rows || []).forEach(function (row) {
        var valid_time;

        row.metrics.forEach(function (metricValues) {
            var properties = {};
            dimensions.forEach(function (dimension, idx) {
                var value = row.dimensions[idx];

                if (dimension === 'ga:dateHour') {
                    dimension = '_time';
                    // Sometimes GA group it as 'Others'
                    valid_time = value.indexOf('20') == 0;
                    value = value.substring(0, 4) + '-' + value.substring(4, 6) + '-' + value.substring(6, 8) + "T" + value.substring(8, 10) + ":00:00";
                }
                if (value !== '(not set)') {
                    properties[dimension] = value;
                }
            });

            metrics.forEach(function (metric, idx) {
                properties[metric] = metricMappers[idx](metricValues.values[idx]);
            });

            if (valid_time) {
                events.push({collection: parameters.collection, properties: properties});
            }
        });
    });

    if (report.nextPageToken && report.nextPageToken) {
        eventStore.store(events);
        config.set('cursor', endDate + " " + report.nextPageToken);
        return [parameters, startDate, endDate, report.nextPageToken, accessKey]
    }

    eventStore.store(events);
    config.set('cursor', null);

    var now = new Date();
    now.setDate(now.getDate() - 1);
    var nowStr = now.toJSON().slice(0, 10);

    if (nowStr != endDate) {
        return [parameters, endDate, null, null, accessKey];
    }
}

var main = function (parameters) {
    var nextCall = fetch(parameters);
    while (nextCall != null) {
        nextCall = fetch(nextCall[0], nextCall[1], nextCall[2], nextCall[3], nextCall[4])
    }
}