var api_url = 'https://person-stream.clearbit.com/v2/combined/find?email=';

var emailRegex = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;

var mapper = function (eventHandler, requestParams, sourceAddress, responseHeaders, parameters) {
    var events = eventHandler.events();
    while (events.hasNext()) {
        var event = events.next();
        if (!emailRegex.test(event.get(parameters.email_field))) {
            continue;
        }
        var response = http.get(api_url + event.get(parameters.email_field))
            .header('Authorization', 'Bearer ' + parameters.api_key).send();
        if (response.getStatusCode() != 200) {
            return;
        }

        var data = JSON.parse(response.getResponseBody()).person;
        event.setOnce('_timezone', data.timeZone);
        event.setOnce('_country_code', data.geo.countryCode);
        event.setOnce('_city', data.geo.city);
        event.setOnce('_latitude', data.geo.lat);
        event.setOnce('_longitude', data.geo.lng);
        event.setOnce('user_job', data.employment.title);
        event.setOnce('user_gender', data.gender);

        event.setOnce('company', data.company || data.employment.name);
        event.setOnce('user_role', data.employment.role);
        event.setOnce('user_seniority', data.employment.seniority);
    }
}