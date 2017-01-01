var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/google";
var report_url = "https://analyticsreporting.googleapis.com/v4/reports:batchGet";

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
        startDate.setMonth(startDate.getMonth() - 2);
        startDate = startDate.toJSON().slice(0, 10);
    }

    if (startDate === endDate) {
        logger.info("No data to process");
        return;
    }

    var response = http.get(oauth_url)
        .query('refresh_token', parameters.refresh_token)
        .send();

    if (response.getStatusCode() == 0) {
        throw new Error(response.getResponseBody());
    }

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var token = response.getResponseBody();
    var metrics = parameters.metrics.split(',');
    var dimensions = parameters.dimensions.split(',');
    response = http.post(report_url)
        .header('Authorization', token)
        .data(JSON.stringify({
            reportRequests: [
                {
                    viewId: parameters.profile_id,
                    dateRanges: [{"startDate": startDate, "endDate": endDate}],
                    metrics: metrics.map(function (metric) { return {"expression": metric} }),
                    dimensions: dimensions.map(function (dim) { return {"name": dim} }),
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

    var events = [];
    var metricMappers = data.reports[0].columnHeader
        .metricHeader.metricHeaderEntries.map(function (item) {
            if (item.type == 'INTEGER') {
                return parseInt;
            }
            else if (item.type == 'CURRENCY') {
                return parseFloat;
            }
            else {
                return function (a) { return a };
            }
        });
    data.reports[0].data.rows.forEach(function (row) {
        row.metrics.forEach(function (metricValues) {

            var properties = {};
            dimensions.forEach(function (dimension, idx) {
                var value = row.dimensions[idx];

                if (dimension == 'ga:date') {
                    dimension = '_time';
                    value = value.substring(0, 4) + '-' + value.substring(4, 6) + '-' + value.substring(6, 8);
                }
                if (value !== '(not set)') {
                    properties[dimension] = value;
                }

            });

            metrics.forEach(function (metric, idx) {
                properties[metric] = metricMappers[idx](metricValues.values[idx]);
            });

            events.push({collection: parameters.collection, properties: properties});
        });
    })

    eventStore.store(events);
    config.set('start_date', endDate);
}

var main = function (parameters) {
    return fetch(parameters, [])
}