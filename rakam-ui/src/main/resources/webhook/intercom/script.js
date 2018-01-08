//@ sourceURL=rakam-ui/src/main/resources/webhook/intercom/script.js

var mapUser = function (properties, user) {
    properties.user_intercom_id = user.id;
    properties.user_id = user.user_id;
    properties.user_anonymous = user.anonymous;
    properties.user_email = user.email;
    properties.user_name = user.name;
    var companies = user.companies;
    if (companies && companies.companies && companies.companies.length > 0) {
        properties.user_companies = [];
        for (var company in companies.companies) {
            properties.user_companies.push(company.id);
        }
    }

    properties.user_signed_up_at = user.signed_up_at;
    properties.user_session_count = user.session_count;
    properties._ip = user.last_seen_ip;
    properties.user_signed_up_at = user.signed_up_at;
    properties._user_agent = user.user_agent_data;
    properties.user_session_count = user.session_count;
    properties._user_agent = user.user_agent_data;

    var tags = user.tags;
    if (tags && tags.tags && tags.tags.length > 0) {
        properties.user_tags = [];
        for (var tag in tags.tags) {
            properties.user_tags.push(tag.id);
        }
    }

    var segments = user.segments;
    if (segments && segments.segments && segments.segments.length > 0) {
        properties.user_segments = [];
        for (var segment in segments.segments) {
            properties.user_segments.push(segments.segments[segment].id);
        }
    }

    var props = user.custom_attributes;
    properties.user_custom_attributes = {};
    if (props) {
        properties.user_custom_attributes = props;
        // cast values to string
        for (var attr in props) {
            properties.user_custom_attributes[attr] = new String(props[attr]);
        }
    }
}

var module = function (queryParams, body, params, headers) {
    if (!body) return;

    var body = JSON.parse(body);
    if (!body || body.topic == 'ping') {
        return null;
    }

    if (params.app_id && body.app_id !== params.app_id) {
        return null;
    }

    if (params.hub_secret) {
        var signature = headers['X-Hub-Signature'];
        if (signature !== ('sha1=' + util.crypt.sha1(params.hub_secret))) {
            return null;
        }
    }

    var properties = {
        _time: body.created_at * 1000
    };

    properties.topic = body.topic;
    if (body.data.item.tag) {
        properties.tag_name = body.data.item.tag.name
    }

    if (body.data.item.type == 'event') {
        properties.event_name = body.data.item.event_name;
        //properties.properties = body.data.item.metadata;
        properties.properties = {};

        var attrs = body.data.item.properties;
        if (attrs) {
            // cast values to string
            for (var key in attrs) {
                properties.properties[key] = new String(attrs[key]);
            }
        }
    }

    if (body.data.item.type == 'user') {
        mapUser(properties, body.data.item);
    }
    else if (body.data.item.type == 'visitor') {
        mapUser(properties, body.data.item);
    }
    else if (body.data.item.type == 'contact') {
        mapUser(properties, body.data.item);
    }
    else if (body.data.item.type == 'conversation') {
        properties.assignee_type = body.data.item.assignee.type;
        properties.assignee_id = body.data.item.assignee.id;

        properties.subject = body.data.item.conversation_message.subject;
        properties.author_type = body.data.item.conversation_message.author.type;
        properties.author_id = body.data.item.conversation_message.author.id;
        properties.body = body.data.item.conversation_parts.body;
    }

    if (body.data.item.user) {
        mapUser(properties, body.data.item.user)
    }

    return {
        collection: params.collection_prefix + '_' + body.data.item.type,
        properties: properties
    }
}