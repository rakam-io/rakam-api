Rakam
=======
An high level distributed analytics service with stream processing capabilities.

Please note that it’s currently under development and not ready for production.

Requirements
------------
* Vert.x 2.x
* Kume
* Cassandra 2.x

Features / Goals
------------
Rakam is a modular data-warehouse API supports pre-aggregation and real-time features.
Rakam collects your data and continuously aggregate it by using your pre-aggregation rules. Also you can run a batch data analysis job on your saved data and combine the results to your newly created pre-aggregation rule.

TODO
------------
* UDP and TCP (Thrift interface) data collection API.
* Distributed PostgreSQL database support as an alternative to Cassandra.
* Event mapper plugins (ip-to-geolocation, social media profile mapper)
* Funnel implementation
* Retention implementation
* Exactly-once event processing 

Contribution
------------
Rakam is my side-project. If you want to contribute the project or suggest an idea feel free to fork it or create a ticket for your suggestion.
