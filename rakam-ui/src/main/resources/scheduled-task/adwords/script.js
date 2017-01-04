var parseCSV=function(r){for(var n,e,f,i=function(r,n,e){return e},o=r.split(""),t=0,l=o.length,s=[];l>t;){for(s.push(f=[]);l>t&&"\r"!==o[t]&&"\n"!==o[t];){if(n=e=t,'"'===o[t]){for(n=e=++t;l>t;){if('"'===o[t]){if('"'!==o[t+1])break;o[++t]=""}e=++t}for('"'===o[t]&&++t;l>t&&"\r"!==o[t]&&"\n"!==o[t]&&","!==o[t];)++t}else for(;l>t&&"\r"!==o[t]&&"\n"!==o[t]&&","!==o[t];)e=++t;f.push(i(s.length-1,f.length,o.slice(n,e).join(""))),","===o[t]&&++t}"\r"===o[t]&&++t,"\n"===o[t]&&++t}return s};

var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/google";
var report_url = "https://adwords.google.com/api/adwords/reportdownload/v201609";

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

    if(startDate === endDate) {
        logger.info("No data to process");
        return;
    }

    var response = http.get(oauth_url)
        .query('refresh_token', parameters.refresh_token)
        .send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var accessToken = response.getResponseBody();

    response = http.post(report_url)
        .header('Authorization', accessToken)
        .header('clientCustomerId', parameters.customer_id)
        .header('developerToken', parameters.developer_token)
        .header('includeZeroImpressions', parameters.include_zero_impressions ? 'true' : null)
        .form('__fmt', 'CSV')
        .form('__rdquery', parameters.query + ' DURING ' +
            startDate.replace(/-/g, '') + ', ' + endDate.replace(/-/g, ''))
        .send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var data = parseCSV(response.getResponseBody());
    var columns = data[1];

    var events = [];
    var row_count = data.length - 1;
    for (var i = 2; i < row_count; i++) {
        var row = data[i];
        var properties = {};
        for (var c = 0; c < data.length; c++) {
            var value = row[c];
            // http://googleadsdeveloper.blogspot.com.tr/2016/03/announcing-v201603-of-adwords-api.html
            if(value === ' --') {
                value = null;
            }
            properties[columns[c]] = value;
        }
        if (properties.date) {
            properties._time = properties.date + "T00:00:00";
            properties.date = undefined;
        }
        events.push({collection: parameters.collection, properties: properties});
    }

    eventStore.store(events);
    config.set('start_date', endDate);
}

var main = function (parameters) {
    return fetch(parameters, [])
}