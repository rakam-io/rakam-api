//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/pipedrive/script.js

var url = "https://api.pipedrive.com/v1/recents";

var eventsWithCustomFields = ["organization", "person", "activity", "deal", "note", "product"];

var fetchFields = function (type, token) {
    var request = http.get("https://api.pipedrive.com/v1/" + type + ":(key,name)")
        .query("api_token", token)
        .query("limit", "500")
        .send();

    var requestJson = JSON.parse(request.getResponseBody());
    if (request.getStatusCode() != 200 || !requestJson.success) {
        throw new Error("Error fetching " + type + " fields: (" + request.getStatusCode() + ") " + request.getResponseBody());
    }

    var fields = {};
    requestJson.data.forEach(function (field) {
        fields[field.key] = field.name;
    });

    return fields;
}

var fetch = function (parameters, events, startDate, offset) {
    logger.debug("Fetching from " + startDate + " with offset " + offset);
    startDate = startDate || config.get('start_timestamp');

    if (startDate == null) {
        startDate = '1970-01-01 00:00:00';
    }

    if (offset == null) {
        offset = 0;
    }

    var types = {};

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

    if (data.data) {
        data.data.forEach(function (activities) {
            var callback = function (item) {
                if (!item) {
                    return;
                }

                if (item.org_id != null) {
                    item._user = item.org_id;
                }

                if (activities.item == 'note') {
                    if (item.organization && typeof item.organization === 'object') {
                        item.organization = item.organization.name;
                    } else {
                        item.organization = item.organization + "";
                    }
                    if (typeof item.user === 'object') {
                        item._user = item.user.email;
                        item._user_name = item.user.name;
                    } else {
                        item._user = item.user + "";
                    }
                    item.user = undefined;
                } else if (activities.item == 'person') {
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

                var newItem = {};
                if (eventsWithCustomFields.indexOf(activities.item) > -1) {
                    var customFields = types[activities.item];
                    if (customFields == null) {
                        customFields = fetchFields(activities.item + 'Fields', parameters.api_token);
                        types[activities.item] = customFields;
                    }

                    for (var key in item) {
                        if (item.hasOwnProperty(key)) {
                            var field = customFields[key];
                            if (!field) {
                                newItem[key] = item[key];
                            }
                            else {
                                newItem[field] = item[key];
                            }
                        }
                    }
                }
                else {
                    newItem = item;
                }

                newItem._time = item.update_time || item.created;
                newItem._user = activities.id;
                events.push({
                    collection: parameters.collection_prefix + "_" + activities.item,
                    properties: newItem
                });
            };

            if (Array.isArray(activities.data)) {
                activities.data.forEach(callback)
            }
            else {
                callback(activities.data);
            }
        });
    }

    eventStore.store(events);
    if (data.additional_data.last_timestamp_on_page) {
        config.set('start_timestamp', data.additional_data.last_timestamp_on_page);
    }

    if (data.additional_data.pagination.more_items_in_collection) {
        return fetch(parameters, [], startDate, data.additional_data.pagination.next_start);
    }
}

var main = function (parameters) {
    return fetch(parameters, []);
}