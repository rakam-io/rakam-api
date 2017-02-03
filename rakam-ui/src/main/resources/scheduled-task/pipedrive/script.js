//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/pipedrive/script.js

var url = "https://api.pipedrive.com/v1/recents";

var fetch = function (parameters, events, startDate, offset) {
    logger.debug("Fetching from " + startDate + " with offset " + offset);
    startDate = startDate || config.get('start_timestamp');

    if (startDate == null) {
        startDate = '1970-01-01 00:00:00';
    }

    if (offset == null) {
        offset = 0;
    }

    var personFieldsRequest = http.get("https://api.pipedrive.com/v1/personFields:(key,name)")
        .query("api_token", parameters.api_token)
        .query("limit", "500")
        .send();

    var personFieldsRequestData = JSON.parse(personFieldsRequest.getResponseBody());
    if (personFieldsRequest.getStatusCode() != 200 || !personFieldsRequestData.success) {
        logger.error("Error fetching person fields: " + personFieldsRequest.getResponseBody());
        return;
    }
    var peopleFields = {};
    personFieldsRequestData.data.forEach(function(field) {
        peopleFields[field.key] = field.name;
    });

    var organizationFieldsRequest = http.get("https://api.pipedrive.com/v1/organizationFields:(key,name)")
        .query("api_token", parameters.api_token)
        .query("limit", "500")
        .send();

    var organizationFieldsRequestData = JSON.parse(organizationFieldsRequest.getResponseBody());
    if (organizationFieldsRequest.getStatusCode() != 200 || !organizationFieldsRequestData.success) {
        logger.error("Error fetching organization fields: " + personFieldsRequest.getResponseBody());
        return;
    }

    var organizationFields = {};
    organizationFieldsRequestData.data.forEach(function(field) {
        organizationFields[field.key] = field.name;
    });

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

                var newItem = {};
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

                    for (var key in item) {
                        if (item.hasOwnProperty(key)) {
                            var peopleField = peopleFields[key];
                            if(!peopleField) {
                                newItem[key] = item[key];
                            } else {
                                newItem[peopleField] = item[key];
                            }
                        }
                    }
                } else
                if (activities.item == 'organization') {
                    for (var key in item) {
                        if (item.hasOwnProperty(key)) {
                            var organizationField = organizationFields[key];
                            if(!organizationField) {
                                newItem[key] = item[key];
                            } else {
                                newItem[organizationField] = item[key];
                            }
                        }
                    }
                } else {
                    newItem = item;
                }

                item._time = item.update_time || item.created;
                item._user = activities.id;
                events.push({
                    collection: parameters.collection_prefix + "_" + activities.item,
                    properties: newItem});
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
    config.set('start_timestamp', data.additional_data.last_timestamp_on_page);

    if (data.additional_data.pagination.more_items_in_collection) {
        return fetch(parameters, [], startDate, data.additional_data.pagination.next_start);
    }
}

var main = function (parameters) {
    return fetch(parameters, []);
}