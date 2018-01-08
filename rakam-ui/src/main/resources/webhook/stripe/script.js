//@ sourceURL=rakam-ui/src/main/resources/stripe/stripe/script.js

var module = function (queryParams, body, params, headers) {
    if (!body) return;
    var properties = {
        _time: body.created * 1000,
        type: body.type
    };

    if (body.type && body.type.startsWith && body.type.startsWith('invoice.')) {
        if (!body.data || !body.data.object) return;
        var object = body.data.object;
        properties['amount_due'] = object['amount_due'];
        properties['attempt_count'] = object['attempt_count'];
        properties['charge'] = object['charge'];
        properties['currency'] = object['currency'];
        properties['customer'] = object['customer'];
        properties['_time'] = object['date'] * 1000;
        properties['discount'] = object['discount'] * 1000;
        properties['ending_balance'] = object['ending_balance'] * 1000;
        properties['total_count'] = object['total_count'];
        properties['lines_id'] = object.lines.data.map(function (row) {
            return row.id;
        });
        properties['lines_amount'] = object.lines.data.map(function (row) {
            return row.amount;
        });
        properties['lines_quantity'] = object.lines.data.map(function (row) {
            return row.quantity;
        });
        properties['lines_type'] = object.lines.data.map(function (row) {
            return row.type;
        });
        properties['lines_currency'] = object.lines.data.map(function (row) {
            return row.currency;
        });
        properties['lines_plan_id'] = object.lines.data.map(function (row) {
            return row.plan.id;
        });
    } else {
        return null;
    }

    return {
        collection: params.collection_prefix,
        properties: properties
    }
}