//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/facebook-ads/script.js

var nested_properties = [
    'cost_per_action_type', 'cost_per_10_sec_video_view', 'action_values', 'cost_per_unique_action_type',
    'unique_actions', 'actions', 'website_ctr', 'video_10_sec_watched_actions',
    'video_15_sec_watched_actions', 'video_30_sec_watched_actions', 'relevance_score',
    'video_avg_pct_watched_actions', 'video_avg_percent_watched_actions', 'video_avg_sec_watched_actions',
    'video_avg_time_watched_actions', 'video_complete_watched_actions', 'video_p100_watched_actions',
    'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions',
    'video_p95_watched_actions'];

var double_fields = ["cost_per_inline_link_click", "cost_per_inline_post_engagement", "inline_post_engagement",
    "cost_per_total_action", "cost_per_estimated_ad_recallers",
    "cost_per_unique_click", "cost_per_unique_inline_link_click", "cpc", "cpm", "cpp", "ctr",
    "frequency", "inline_link_click_ctr", "newsfeed_avg_position",
    "social_spend", "spend", "unique_ctr", "unique_inline_link_click_ctr", "unique_link_clicks_ctr"];
var long_fields = ["app_store_clicks", "call_to_action_clicks",
    "canvas_avg_view_percent", "canvas_avg_view_time",
    "clicks", "deeplink_clicks", "estimated_ad_recall_rate",
    "impressions", "reach", "social_clicks",
    "social_impressions", "social_reach", "total_actions",
    "total_unique_actions", "unique_clicks", "unique_impressions",
    "unique_inline_link_clicks", "unique_social_clicks", "unique_social_clicks", "website_clicks"];
var valueMapper = {};
double_fields.forEach(function (key) {
    valueMapper[key] = parseFloat;
});
long_fields.forEach(function (key) {
    valueMapper[key] = parseInt;
});

// date_stop is not required since we fetch the reports grouped by day
var fields = "account_id,account_name,action_values,actions,ad_id,ad_name,adset_id,adset_name,app_store_clicks,buying_type,call_to_action_clicks,campaign_id,campaign_name,canvas_avg_view_percent,canvas_avg_view_time,clicks,cost_per_10_sec_video_view,cost_per_action_type,cost_per_estimated_ad_recallers,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_total_action,cost_per_unique_action_type,cost_per_unique_click,cost_per_unique_inline_link_click,cpc,cpm,cpp,ctr,date_start,deeplink_clicks,estimated_ad_recall_rate,estimated_ad_recallers,frequency,impressions,inline_link_click_ctr,inline_link_clicks,inline_post_engagement,objective,place_page_name,reach,relevance_score,social_clicks,social_impressions,social_reach,social_spend,spend,total_action_value,total_actions,total_unique_actions,unique_actions,unique_clicks,unique_ctr,unique_inline_link_click_ctr,unique_inline_link_clicks,unique_link_clicks_ctr,unique_social_clicks,video_10_sec_watched_actions,video_15_sec_watched_actions,video_30_sec_watched_actions,video_avg_percent_watched_actions,video_avg_time_watched_actions,video_p100_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,website_clicks,website_ctr";

var internal = function (parameters, url, events, day) {
    logger.debug("Fetching " + day + (url ? ' with cursor' : ''));

    url = url || "https://graph.facebook.com/v2.9/act_" + parameters.account_id + "/insights?level=ad&time_increment=1&access_token=" + parameters.access_token + "&fields=" + fields + "&format=json&breakdowns=" + parameters.breakdowns + "&time_range={\"since\":\"" + day + "\",\"until\":\"" + day + "\"}";

    var response = http.get(url).send();
    if (response.getStatusCode() == 0) {
        throw new Error(response.getResponseBody());
    }
    var data = JSON.parse(response.getResponseBody());

    if (response.getStatusCode() != 200) {
        throw new Error(JSON.stringify(data.error.code + ' : ' + data.error.error_subcode + ' : ' + data.error.message));
    }

    data.data.forEach(function (campaign) {
        nested_properties.forEach(function (type) {
            var object = {};
            if (campaign[type]) {
                campaign[type].forEach(function (row) {
                    object[row.action_type] = parseFloat(row.value);
                });
            }
            campaign[type] = object;
        });

        for (var key in valueMapper) {
            campaign[key] = valueMapper[key](campaign[key])
        }

        campaign._time = campaign.date_start + "T00:00:00";
        events.push({collection: parameters.collection, properties: campaign});
    });

    return data;
}

var main = function (parameters) {
    parameters.breakdowns = parameters.breakdowns || "age,gender";

    var endDate = new Date();
    endDate.setDate(endDate.getDate() - 1);
    endDate = endDate.toJSON().slice(0, 10);

    var startDate = config.get('start_date');

    if (startDate == null) {
        startDate = new Date(parameters.start_date || '2016-12-01');
    }
    else {
        startDate = new Date(startDate);
        startDate.setDate(startDate.getDate() + 1);
    }

    var url = null;
    while (!(new Date(endDate) < new Date(startDate))) {
        var current_day = startDate.toJSON().slice(0, 10);
        var events = [];
        while (true) {
            var data = internal(parameters, url, events, current_day);

            if (data.paging && data.paging.next) {
                url = data.paging.next;
                continue;
            }
            else {
                url = null;
                break;
            }
        }

        config.set('start_date', current_day);
        eventStore.store(events);
        startDate.setDate(startDate.getDate() + 1);
    }
}