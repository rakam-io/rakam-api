[![Stories in Ready](https://badge.waffle.io/buremba/rakam.png?label=ready&title=Ready)](https://waffle.io/buremba/rakam)
Rakam
=======

[![Join the chat at https://gitter.im/buremba/rakam](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/buremba/rakam?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
A high level distributed analytics service with stream processing capabilities.

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
It collects your data, saves in a columnar database and continuously aggregate it by using your pre-aggregation rules.

TODO
------------
* UDP and TCP data collection API.
* Distributed PostgreSQL database support as an alternative to Kafka & Hive.
* Event mapper plugins (ip-to-geolocation, social media profile mapper)
* Funnels implementation
* Retention implementation
* Trigger API

Contribution
------------
Rakam is my side-project. If you want to contribute the project or suggest an idea feel free to fork it or create a ticket for your suggestion.
