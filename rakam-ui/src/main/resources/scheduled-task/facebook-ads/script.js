//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/facebook-ads/script.js

var nested_properties = [
    'cost_per_action_type', 'cost_per_10_sec_video_view', 'action_values', 'cost_per_unique_action_type',
    'unique_actions', 'actions', 'website_ctr', 'video_10_sec_watched_actions',
    'video_15_sec_watched_actions', 'video_30_sec_watched_actions',
    'video_avg_pct_watched_actions', 'video_avg_percent_watched_actions', 'video_avg_sec_watched_actions',
    'video_avg_time_watched_actions', 'video_complete_watched_actions', 'video_p100_watched_actions',
    'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions',
    'video_p95_watched_actions'];

var double_fields = ["cost_per_inline_link_click", "cost_per_inline_post_engagement",
                     "cost_per_total_action", "cost_per_estimated_ad_recallers",
                     "cost_per_unique_click", "cost_per_unique_inline_link_click", "cpc", "cpm", "cpp", "ctr",
                     "frequency", "inline_link_click_ctr", "newsfeed_avg_position",
                     "social_spend", "spend", "unique_ctr", "unique_inline_link_click_ctr", "unique_link_clicks_ctr"];
var long_fields = ["app_store_clicks", "call_to_action_clicks",
                   "canvas_avg_view_percent", "canvas_avg_view_time",
                   "clicks", "deeplink_clicks", "estimated_ad_recall_rate",
                   "impressions", "inline_link_click_ctr", "inline_link_click_ctr",
                   "newsfeed_avg_position", "newsfeed_avg_position", "reach", "social_clicks",
                   "social_impressions", "social_reach", "spend", "total_actions",
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
var fields = "account_id,account_name,action_values,actions,ad_id,ad_name,adset_id,adset_name,app_store_clicks,buying_type,call_to_action_clicks,campaign_id,campaign_name,canvas_avg_view_percent,canvas_avg_view_time,clicks,cost_per_10_sec_video_view,cost_per_action_type,cost_per_estimated_ad_recallers,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_total_action,cost_per_unique_action_type,cost_per_unique_click,cost_per_unique_inline_link_click,cpc,cpm,cpp,ctr,date_start,deeplink_clicks,estimated_ad_recall_rate,estimated_ad_recallers,frequency,impressions,inline_link_click_ctr,inline_link_clicks,inline_post_engagement,newsfeed_avg_position,newsfeed_clicks,newsfeed_impressions,objective,place_page_name,reach,relevance_score,social_clicks,social_impressions,social_reach,social_spend,spend,total_action_value,total_actions,total_unique_actions,unique_actions,unique_clicks,unique_ctr,unique_impressions,unique_inline_link_click_ctr,unique_inline_link_clicks,unique_link_clicks_ctr,unique_social_clicks,unique_social_impressions,video_10_sec_watched_actions,video_15_sec_watched_actions,video_30_sec_watched_actions,video_avg_pct_watched_actions,video_avg_percent_watched_actions,video_avg_sec_watched_actions,video_avg_time_watched_actions,video_complete_watched_actions,video_p100_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,website_clicks,website_ctr";

var fetch = function (parameters, url, events, startDate, endDate) {
    if (endDate == null) {
        endDate = new Date();
        endDate.setDate(endDate.getDate() - 1);
        endDate = endDate.toJSON().slice(0, 10);
    }

    startDate = startDate || config.get('start_date');

    if (startDate == null) {
        startDate = new Date();
        startDate.setYear(2015);
        startDate = startDate.toJSON().slice(0, 10);
    }

    logger.debug("Fetching between " + startDate + " and " + (endDate || 'now') + (url ? ' with cursor' : ''));

    if (!(new Date(endDate) > new Date(startDate))) {
        config.set('start_date', endDate);
        return;
    }

    var urlIsGenerated = url == null;
    if (urlIsGenerated) {
        var endGap = new Date(endDate);
        endGap.setDate(endGap.getDate() - 1);
        url = "https://graph.facebook.com/v2.8/act_" + parameters.account_id + "/insights?level=ad&time_increment=1&access_token=" + parameters.access_token + "&fields=" + fields + "&format=json&limit=250&breakdowns=age,gender&time_range={\"since\":\"" + startDate + "\",\"until\":\"" + endGap.toJSON().slice(0, 10) + "\"}";
    }
    var response = http.get(url).send();
    if (response.getStatusCode() == 0) {
        throw new Error(response.getResponseBody());
    }
    var data = JSON.parse(response.getResponseBody());

    if (response.getStatusCode() != 200) {
        if (data.error.code === 1) {
            if (!urlIsGenerated) {
                throw new Error("Facebook halted: " + JSON.stringify(data.error));
            }
            logger.warn("The date range is probably too big, here the the Facebook response: " + data.error.message);
            var gap = ((new Date(endDate).getTime() - new Date(startDate).getTime()) / 2) / 1000 / 86400;
            var mid = new Date(startDate);
            mid.setDate(mid.getDate() + gap);
            var midText = mid.toJSON().slice(0, 10);
            logger.debug("The date range is cut half two equal slices " + startDate + "-" + midText + " and " + midText + "-" + endDate);
            fetch(parameters, null, [], startDate, midText);
            fetch(parameters, null, [], midText, endDate);
        }
        else {
            logger[data.error.code === 17 ? 'warn' : 'error'](JSON.stringify(data.error.code + ' : ' + data.error.error_subcode + ' : ' + data.error.message));
        }
        return;
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
        campaign._user = campaign.account_id;
        campaign.account_id = undefined;
        events.push({collection: parameters.collection, properties: campaign});
    });

    if (data.paging && data.paging.next) {
        return fetch(parameters, data.paging.next, events, startDate, endDate);
    }
    else {
        eventStore.store(events);
        config.set('start_date', endDate);
    }
}

var main = function (parameters) {
    return fetch(parameters, null, [])
}