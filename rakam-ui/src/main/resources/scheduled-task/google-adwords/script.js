//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/adwords/script.js
var parseCSV = function (r) {
    for (var n, e, f, i = function (r, n, e) {
        return e
    }, o = r.split(""), t = 0, l = o.length, s = []; l > t;) {
        for (s.push(f = []); l > t && "\r" !== o[t] && "\n" !== o[t];) {
            if (n = e = t, '"' === o[t]) {
                for (n = e = ++t; l > t;) {
                    if ('"' === o[t]) {
                        if ('"' !== o[t + 1]) break;
                        o[++t] = ""
                    }
                    e = ++t
                }
                for ('"' === o[t] && ++t; l > t && "\r" !== o[t] && "\n" !== o[t] && "," !== o[t];) ++t
            } else for (; l > t && "\r" !== o[t] && "\n" !== o[t] && "," !== o[t];) e = ++t;
            f.push(i(s.length - 1, f.length, o.slice(n, e).join(""))), "," === o[t] && ++t
        }
        "\r" === o[t] && ++t, "\n" === o[t] && ++t
    }
    return s
};

var doubleValues = ["impressions", "clicks", "engagements", "conversions", "all conv.", "interactions", "views", "average position", "avg. position", "all conv."];
var percentageValues = ['% new sessions', 'search lost is (rank)', 'content lost is (rank)', 'search impr. share', 'content impr. share',
    'video played to 25%', 'video played to 50%', 'video played to 75%', 'video played to 100%'];
var moneyValues = ["cost", "avg. cost", "avg. cpm"];

var valueMapper = {};
doubleValues.forEach(function (key) {
    valueMapper[key] = parseFloat;
});
moneyValues.forEach(function (key) {
    valueMapper[key] = function (value) {
        return parseFloat(value) / 1000000;
    };
});
var parsePercentage = function (value) {
    return parseInt(value.substring(0, value.length - 1));
}

percentageValues.forEach(function (key) {
    valueMapper[key] = parsePercentage;
});

var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/google";
var report_url = "https://adwords.google.com/api/adwords/reportdownload/v201708";

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
        startDate.setMonth(startDate.getMonth() - 1);
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

    var accessToken = response.getResponseBody();


    var endGap = new Date(endDate);
    endGap.setDate(endGap.getDate() - 1);
    response = http.post(report_url)
        .header('Authorization', accessToken)
        .header('clientCustomerId', parameters.customer_id)
        .header('developerToken', parameters.developer_token)
        .header('includeZeroImpressions', parameters.include_zero_impressions)
        .form('__fmt', 'CSV')
        .form('__rdquery', parameters.query + ' DURING ' +
            startDate.replace(/-/g, '') + ', ' + endGap.toJSON().slice(0, 10).replace(/-/g, ''))
        .send();

    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var data = parseCSV(response.getResponseBody());
    var columns = data[1].map(function (col) {
        return col.toLowerCase();
    });

    var events = [];
    var row_count = data.length - 1;
    for (var i = 2; i < row_count; i++) {
        var row = data[i];
        var properties = {};
        for (var c = 0; c < columns.length; c++) {
            var value = row[c];
            if (value === null) {
                continue;
            }
            // http://googleadsdeveloper.blogspot.com.tr/2016/03/announcing-v201603-of-adwords-api.html
            if (value === ' --') {
                continue;
            }

            var key = columns[c];
            var mapper = valueMapper[key];
            properties[key] = mapper ? mapper(value) : value;
        }
        if (properties.day) {
            properties._time = properties.day + "T00:00:00";
            properties.day = undefined;
        }
        events.push({collection: parameters.collection, properties: properties});
    }

    eventStore.store(events);
    config.set('start_date' + (index == null ? "" : "." + index), endDate);
}

var adgroup_query = "SELECT Date,AccountCurrencyCode,Clicks,AllConversions,AdGroupName,AverageCost,AverageCpm,AveragePosition,CampaignName,Conversions,Cost,Engagements,Impressions,Interactions,PercentNewVisitors,VideoViews,VideoQuartile100Rate,VideoQuartile75Rate,VideoQuartile50Rate,VideoQuartile25Rate,ContentImpressionShare,ContentRankLostImpressionShare from ADGROUP_PERFORMANCE_REPORT";
var ad_query = "SELECT Date,AccountCurrencyCode,AdGroupName,AdType,AllConversions,AverageCost,AverageCpm,AveragePosition,CampaignName,Clicks,Conversions,Cost,CriterionType,DisplayUrl,Engagements,Headline,Impressions,Interactions,PercentNewVisitors,VideoViews,VideoQuartile100Rate,VideoQuartile75Rate,VideoQuartile50Rate,VideoQuartile25Rate FROM AD_PERFORMANCE_REPORT";

var main = function (parameters) {
    if (parameters.query) {
        return fetch(parameters, []);
    } else {
        parameters.collection += "_adgroup";
        parameters.query = adgroup_query;
        fetch(parameters, [], 0);

        parameters.collection += "_ad";
        parameters.query = ad_query;
        fetch(parameters, [], 1);
    }
}