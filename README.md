Rakam
=======
An high level distributed analytics service with stream processing capabilities.

Please note that it’s currently under development and not ready for production.

Requirements
------------
* Kume
* PrestoDB
* Hive
* Kafka

Features / Goals
------------
Rakam is a modular data-warehouse API supports pre-aggregation and real-time features.
Rakam collects your data, saves in a columnar database and continuously aggregate it by using your pre-aggregation rules.

TODO
------------
* UDP and TCP data collection API.
* Distributed PostgreSQL database support as an alternative to Kafka - Hive database.
* Event mapper plugins (ip-to-geolocation, social media profile mapper)
* Funnels implementation
* Retention implementation
* Trigger API

Contribution
------------
Rakam is my side-project. If you want to contribute the project or suggest an idea feel free to fork it or create a ticket for your suggestion.
