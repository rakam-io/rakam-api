//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/pipedrive/script.js

var url = "https://api.pipedrive.com/v1/recents";

var fetch = function (parameters, events, startDate, offset) {
    logger.debug("Fetching from " + startDate + " with offset " + offset);
    startDate = startDate || config.get('start_timestamp');

    if (startDate == null) {
        startDate = new Date(0).toISOString();
    }

    if (offset == null) {
        offset = 0;
    }

    var response = http.get(url)
        .query("api_token", parameters.api_token)
        .query("limit", "500")
        .query("start", offset)
        .query("since_timestamp", startDate)
        .send();

    if (response.getStatusCode() == 0) {
        throw new Error(response.getResponseBody());
    }
    var data = JSON.parse(response.getResponseBody());

    if (response.getStatusCode() != 200 || !data.success) {
        logger.error("Error: " + response.getResponseBody());
        return;
    }

    data.data.forEach(function (activities) {
        var callback = function (item) {
            if (!item) {
                return;
            }
            if (activities.item == 'person') {
                if (typeof item.email === 'object') {
                    item.email = item.email.value;
                }
                if (Array.isArray(item.phone)) {
                    var phone;
                    for (var i = 0; i < item.phone.length; i++) {
                        var p = item.phone[i];
                        phone = p.value;
                        if (p.primary) {
                            break;
                        }
                    }
                    item.phone = phone
                }
            }
            item._time = item.update_time || item.created;
            item._user = activities.id;
            events.push({collection: parameters.collection_prefix + "_" + activities.item, properties: item});
        };

        if (Array.isArray(activities.data)) {
            activities.data.forEach(callback)
        }
        else {
            callback(activities.data);
        }
    });

    eventStore.store(events);
    config.set('start_timestamp', data.additional_data.last_timestamp_on_page);

    if (data.additional_data.pagination.more_items_in_collection) {
        return fetch(parameters, [], startDate, data.additional_data.pagination.next_start);
    }
}

var main = function (parameters) {
    return fetch(parameters, []);
}