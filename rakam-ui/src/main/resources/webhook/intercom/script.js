var module = function(queryParams, body, params, headers) {
    if(!body || body.topic == 'ping') {
        return null;
    }

    if(params.app_id && body.app_id !== params.app_id) {
        return null;
    }

    if(params.hub_secret) {
        var signature = headers['X-Hub-Signature'];
        if(signature !== ('sha1=' + Java.type('org.rakam.util.CryptUtil').sha1(params.hub_secret))) {
            return null;
        }
    }

    var properties = {
        _time: body.created_at * 1000
    };

    properties.topic = body.topic;
    if(body.data.item.tag) {
        properties.tag_name = body.data.item.tag.name
    }

    if(body.data.item.type == 'event') {
        properties.event_name = body.data.item.event_name;
        properties.properties = body.data.item.metadata;
        properties.properties = body.data.item.properties;
        var attrs = body.data.item.properties;
        if(attrs) {
            // cast values to string
            for (var key in attrs) {
                attrs[key] = new String(attrs[key]);
            }
        }
    }

    if(body.data.item.type == 'conversation') {
        properties.assignee_type = body.data.item.assignee.type;
        properties.assignee_id = body.data.item.assignee.id;

        properties.subject = body.data.item.conversation_message.subject;
        properties.author_type = body.data.item.conversation_message.author.type;
        properties.author_id = body.data.item.conversation_message.author.id;
    }

    if(body.data.item.user) {
        properties.user_intagram_id = body.data.item.user.id;
        properties.user_id = body.data.item.user.user_id;
        properties.user_anonymous = body.data.item.user.anonymous;
        properties.user_email = body.data.item.user.email;
        properties.user_name = body.data.item.user.name;
        var companies = body.data.item.user.companies;
        if(companies && companies.companies && companies.companies.length > 0) {
            properties.user_companies = [];
            for (var company in companies.companies) {
                properties.user_companies.push(company.id);
            }
        }
        var location = body.data.item.user.location_data;
        if(location) {
            for (var key in location) {
                properties['user_'+key] = location[key];
            }
        }

        properties.user_signed_up_at = body.data.item.user.signed_up_at;
        properties.user_session_count = body.data.item.user.session_count;
        properties._user_agent = body.data.item.user.user_agent_data;
        var tags = body.data.item.user.tags;
        if(tags && tags.tags && tags.tags.length > 0) {
            properties.user_tags = [];
            for (var tag in tags.tags) {
                properties.user_tags.push(tag.id);
            }
        }

        var segments = body.data.item.user.segments;
        if(segments && segments.segments && segments.segments.length > 0) {
            properties.user_segments = [];
            for (var segment in segments.segments) {
                properties.user_segments.push(segment.id);
            }
        }

        var props = body.data.item.user.custom_attributes;
        if(props) {
            properties.user_custom_attributes = props;
            // cast values to string
            for (var attr in props) {
                attrs[attr] = new String(props[attr]);
            }
        }
    }


    return {
        collection: params.collection_prefix + '_' + body.data.item.type,
        properties: properties
    }
}