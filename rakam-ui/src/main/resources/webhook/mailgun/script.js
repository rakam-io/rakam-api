//@ sourceURL=rakam-ui/src/main/resources/webhook/mailgun/script.js

var module = function (queryParams, _body, params, headers) {
    if (!_body) return;
    var body = util.request.parseFormData(_body);
    if (!body.signature || !body.timestamp || !body.recipient || !body.domain) return;

    if (body.signature[0] !== util.crypt.encryptToHex(body.timestamp[0] + body.token[0], params.api_key, 'HmacSHA256')) {
        throw Error('Signature is invalid');
    }

    var properties = {
        _time: body.timestamp[0] * 1000,
        recipient: body.recipient[0],
        domain: body.domain[0]
    };

    var messageHeaders = body['message-headers'];
    if (messageHeaders && messageHeaders[0]) {
        var headers = JSON.parse(messageHeaders[0]);
        for (var i = 0; i < headers.length; i++) {
            if (headers[i][0] == 'X-Mailgun-Variables') {
                var custom = JSON.parse(headers[i][1]);
                for (var i in custom) {
                    properties['custom_' + i] = custom[i];
                }
            }
        }
    }

    var event = body.event[0];
    properties['event'] = event;
    if (body['campaign-id']) properties['campaign-id'] = body['campaign-id'][0];
    if (body['tag']) properties['tag'] = body['tag'][0];
    if (body['mailing-list']) properties['mailing-list'] = body['mailing-list'][0];

    if (event == 'clicked' || event == 'opened' || event == 'unsubscribed') {
        properties['_ip'] = body['ip'][0];
        properties['_user_agent'] = body['user-agent'][0];
        if (body['url']) {
            properties['url'] = body['url'][0];
        }
    } else if (event == 'dropped') {
        properties['error_reason'] = body['reason'][0];
        properties['error_code'] = body['code'][0];
    } else if (event == 'bounced') {
        properties['error_reason'] = body['reason'][0];
        properties['error_code'] = body['code'][0];
    } else if (event == 'complained') {
        properties['error_reason'] = body['reason'][0];
        properties['error_code'] = body['code'][0];
    }

    return {
        collection: params.collection,
        properties: properties
    }
}