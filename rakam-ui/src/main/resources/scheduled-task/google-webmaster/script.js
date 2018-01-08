//@ sourceURL=rakam-ui/src/main/resources/scheduled-task/google-webmaster/script.js

var countryMapping = {
    "ABW": "AW",
    "AFG": "AF",
    "AGO": "AO",
    "AIA": "AI",
    "ALB": "AL",
    "AND": "AD",
    "ARE": "AE",
    "ARG": "AR",
    "ARM": "AM",
    "ASM": "AS",
    "ATA": "AQ",
    "ATF": "TF",
    "ATG": "AG",
    "AUS": "AU",
    "AUT": "AT",
    "AZE": "AZ",
    "BDI": "BI",
    "BEL": "BE",
    "BEN": "BJ",
    "BFA": "BF",
    "BGD": "BD",
    "BGR": "BG",
    "BHR": "BH",
    "BHS": "BS",
    "BIH": "BA",
    "BLR": "BY",
    "BLZ": "BZ",
    "BMU": "BM",
    "BRA": "BR",
    "BRB": "BB",
    "BRN": "BN",
    "BTN": "BT",
    "BVT": "BV",
    "BWA": "BW",
    "CAF": "CF",
    "CAN": "CA",
    "CCK": "CC",
    "CHE": "CH",
    "CHL": "CL",
    "CHN": "CN",
    "CMR": "CM",
    "COG": "CG",
    "COK": "CK",
    "COL": "CO",
    "COM": "KM",
    "CRI": "CR",
    "CUB": "CU",
    "CXR": "CX",
    "CYM": "KY",
    "CYP": "CY",
    "DEU": "DE",
    "DJI": "DJ",
    "DMA": "DM",
    "DNK": "DK",
    "DOM": "DO",
    "DZA": "DZ",
    "ECU": "EC",
    "EGY": "EG",
    "ERI": "ER",
    "ESH": "EH",
    "ESP": "ES",
    "EST": "EE",
    "ETH": "ET",
    "FIN": "FI",
    "FJI": "FJ",
    "FLK": "FK",
    "FRA": "FR",
    "FRO": "FO",
    "FSM": "FM",
    "GAB": "GA",
    "GBR": "GB",
    "GEO": "GE",
    "GGY": "GG",
    "GHA": "GH",
    "GIB": "GI",
    "GIN": "GN",
    "GLP": "GP",
    "GMB": "GM",
    "GNB": "GW",
    "GNQ": "GQ",
    "GRC": "GR",
    "GRD": "GD",
    "GRL": "GL",
    "GTM": "GT",
    "GUF": "GF",
    "GUM": "GU",
    "GUY": "GY",
    "HKG": "HK",
    "HMD": "HM",
    "HND": "HN",
    "HRV": "HR",
    "HTI": "HT",
    "HUN": "HU",
    "IDN": "ID",
    "IMN": "IM",
    "IND": "IN",
    "IOT": "IO",
    "IRL": "IE",
    "IRN": "IR",
    "IRQ": "IQ",
    "ISL": "IS",
    "ISR": "IL",
    "ITA": "IT",
    "JAM": "JM",
    "JEY": "JE",
    "JOR": "JO",
    "JPN": "JP",
    "KAZ": "KZ",
    "KEN": "KE",
    "KGZ": "KG",
    "KHM": "KH",
    "KIR": "KI",
    "KNA": "KN",
    "KOR": "KR",
    "KWT": "KW",
    "LAO": "LA",
    "LBN": "LB",
    "LBR": "LR",
    "LCA": "LC",
    "LIE": "LI",
    "LKA": "LK",
    "LSO": "LS",
    "LTU": "LT",
    "LUX": "LU",
    "LVA": "LV",
    "MAC": "MO",
    "MAR": "MA",
    "MCO": "MC",
    "MDA": "MD",
    "MDG": "MG",
    "MDV": "MV",
    "MEX": "MX",
    "MHL": "MH",
    "MLI": "ML",
    "MLT": "MT",
    "MMR": "MM",
    "MNE": "ME",
    "MNG": "MN",
    "MNP": "MP",
    "MOZ": "MZ",
    "MRT": "MR",
    "MSR": "MS",
    "MTQ": "MQ",
    "MUS": "MU",
    "MWI": "MW",
    "MYS": "MY",
    "MYT": "YT",
    "NAM": "NA",
    "NCL": "NC",
    "NER": "NE",
    "NFK": "NF",
    "NGA": "NG",
    "NIC": "NI",
    "NIU": "NU",
    "NLD": "NL",
    "NOR": "NO",
    "NPL": "NP",
    "NRU": "NR",
    "NZL": "NZ",
    "OMN": "OM",
    "PAK": "PK",
    "PAN": "PA",
    "PCN": "PN",
    "PER": "PE",
    "PHL": "PH",
    "PLW": "PW",
    "PNG": "PG",
    "POL": "PL",
    "PRI": "PR",
    "PRK": "KP",
    "PRT": "PT",
    "PRY": "PY",
    "PYF": "PF",
    "QAT": "QA",
    "ROU": "RO",
    "RUS": "RU",
    "RWA": "RW",
    "SAU": "SA",
    "SDN": "SD",
    "SEN": "SN",
    "SGP": "SG",
    "SGS": "GS",
    "SJM": "SJ",
    "SLB": "SB",
    "SLE": "SL",
    "SLV": "SV",
    "SMR": "SM",
    "SOM": "SO",
    "SPM": "PM",
    "SRB": "RS",
    "SSD": "SS",
    "STP": "ST",
    "SUR": "SR",
    "SVK": "SK",
    "SVN": "SI",
    "SWE": "SE",
    "SWZ": "SZ",
    "SYC": "SC",
    "SYR": "SY",
    "TCA": "TC",
    "TCD": "TD",
    "TGO": "TG",
    "THA": "TH",
    "TJK": "TJ",
    "TKL": "TK",
    "TKM": "TM",
    "TLS": "TL",
    "TON": "TO",
    "TTO": "TT",
    "TUN": "TN",
    "TUR": "TR",
    "TUV": "TV",
    "TZA": "TZ",
    "UGA": "UG",
    "UKR": "UA",
    "UMI": "UM",
    "URY": "UY",
    "UZB": "UZ",
    "VCT": "VC",
    "VGB": "VG",
    "VIR": "VI",
    "VUT": "VU",
    "WLF": "WF",
    "WSM": "WS",
    "YEM": "YE",
    "ZAF": "ZA",
    "ZMB": "ZM",
    "ZWE": "ZW"
};
var oauth_url = "https://d2p3wisckg.execute-api.us-east-2.amazonaws.com/prod/google";
var report_url = "https://content.googleapis.com/webmasters/v3/sites/%s/searchAnalytics/query?fields=rows&alt=json";
var mainProperties = ["clicks", "impressions", "ctr", "position"];

var fetch = function (parameters, events, startDate, endDate) {
    logger.debug("Fetching between " + startDate + " and " + (endDate || 'now'));
    if (endDate == null) {
        endDate = new Date();
        endDate.setDate(endDate.getDate() - 1);
        endDate = endDate.toJSON().slice(0, 10);
    }

    startDate = startDate || config.get('start_date');

    if (startDate == null) {
        startDate = new Date();
        startDate.setMonth(startDate.getMonth() - 5);
        startDate = startDate.toJSON().slice(0, 10);
    }

    if (startDate === endDate) {
        logger.info("No data to process");
        return;
    }

    var response = http.get(oauth_url)
        .query('refresh_token', parameters.refresh_token)
        .send();
    if (response.getStatusCode() != 200) {
        throw new Error(response.getResponseBody());
    }

    var data = response.getResponseBody();
    response = http.post(report_url.replace('%s', encodeURIComponent(parameters.website_url)))
        .header('Authorization', data)
        .header('Content-Type', "application/json")
        .data(JSON.stringify({
            dimensions: ["date", "country", "device", "page", "query"],
            responseAggregationType: "byPage",
            rowLimit: 5000,
            startDate: startDate, endDate: endDate
        }))
        .send();

    if (response.getStatusCode() != 200) {
        if (response.getStatusCode() == 404) {
            throw new Error("The website " + parameters.website_url + " could not found");
        }

        throw new Error(response.getResponseBody())
    }

    var data = JSON.parse(response.getResponseBody());
    var events = data.rows.map(function (row) {
        var properties = {
            '_time': row.keys[0] + "T00:00:00",
            country: countryMapping[row.keys[1].toUpperCase()],
            device: row.keys[2],
            page: row.keys[3],
            query: row.keys[4]
        }
        mainProperties.forEach(function (term) {
            properties[term] = row[term];
        });

        return {collection: parameters.collection, properties: properties};
    });

    eventStore.store(events);
    config.set('start_date', endDate);
}

var main = function (parameters) {
    return fetch(parameters, [])
}