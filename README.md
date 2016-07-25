[![Build Status](https://travis-ci.org/rakam-io/rakam.svg?branch=master)](https://travis-ci.org/rakam-io/rakam)

Rakam
=======

[![Join the chat at https://gitter.im/rakam-io/rakam](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/buremba/rakam?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Analytics platform that allows you to create your analytics services.

Features / Goals
------------
Rakam is a modular analytics platform that gives you a set of features to create your own analytics service.

Typical workflow of using Rakam:
* Collect data from your clients using **[client libraries](//rakam.io/doc/Client_Libraries)**
* Make your event data richer and cleaner with **[event mappers](//rakam.io/doc/Event-Mappers)**
* Process data in real-time and pre-aggregate using **[continuous queries](//rakam.io/doc)** (Stream processing with SQL!),
* Store data in a data warehouse to analyze it later. (Postgresql, HDFS, S3 or any file-system you want)
* Analyze your event data in your data warehouse with your custom SQL queries and integrated rich analytics features **(funnel queries**, **retention queries**, **[real-time reports](//rakam.io/doc/Modules#realtimeanalyticsmodulesubapidocgetrakamcomapitagsrealtimesub)**, **[event streams](//rakam.io/doc/Modules#eventstreammodulesubapidocgetrakamcomapitagsstreamsub)**)
* Analyze your users with **[integrated CRM tool](//rakam.io/doc/Modules#crmmodulecustomermailboxsubapidocgetrakamcomapitagsusermailboxsub)**
* Visualize your data using **[web application](#Webapplication)** of Rakam similar to BI tools.
* **[Add your own modules](//rakam.io/doc/Developing-Modules)** to Rakam to customize Rakam for your special needs.

All these features come with a single box, you just need to specify which modules you want to use using a configuration file (config.properties) and Rakam will do the rest for you.
You can also start multiple instances and put them behind a load balancer if you need high availability or/and want to collect tens of thousands events per second.

Deployment
------------
There are multiple deployment types depending of your needs.

If your event data-set can fit in a single server, we recommend using Postgresql backend. Rakam will collect all your events in row-oriented format in a Postgresql node. All the features provided by Rakam are supported in Postgresql deployment type.

However Rakam is designed to be highly scalable in order to provide a solution for high work-loads. You can configure Rakam to send events to a distributed commit-log such as Apache Kafka or Amazon Kinesis in serialized Apache Avro format and process data in PrestoDB workers and store them in a distributed filesystem in a columnar format.

### Heroku

You can easily deploy Rakam to Heroku using Heroku button, it adds Heroku Postgresql add-on to your app and use Postgresql deployment type.

[![Deploy](https://www.herokucdn.com/deploy/button.png)](https://heroku.com/deploy)

### Docker

Run the following command to start a Postgresql server in docker container and Rakam API in your local environment.

    docker run -d --name rakam-db -e POSTGRES_PASSWORD=dummy -e POSTGRES_USER=rakam postgres:9.5.3 && docker run -d --name rakam -p 9999:9999 buremba/rakam

After docker container is started, visit [http://127.0.0.1:9999](http://127.0.0.1:9999) and follow the instructions.
You can directly use Rakam API with client libraries or register your Rakam cluster to Rakam BI at [app.rakam.io](https://rakam.io)

We also provide docker-compose definition for Postgresql backend. Create a `docker-compose.yml` with from this definition and run the command  `docker-compose run api -p 9999:9999 -f docker-compose.yml`.

    version: '2'
    services:
      rakam-db:
        image: postgres:9.5.3
        environment:
          - POSTGRES_PASSWORD=dummy
          - POSTGRES_USER=rakam
      rakam-api:
        build: buremba:rakam
        ports:
          - "9999:9999"
        depends_on:
          - db

In order to start docker container for standalone Rakam API, use this command:

    docker run -d --name rakam -p 9999:9999 buremba/rakam

You can set config variables for Rakam instance using environment variables. All properties in config.properties file can be set via environment variable `RAKAM_property_name_dots_replaced_by_underscore`.
For example, if you want to set `store.adapter=postgresql` you need to set environment variable `RAKAM_STORE_ADAPTER=postgresql`.

Dockerfile will generate `config.properties` file from environment variables in docker container that start with `RAKAM_` prefix.

In order to set environment variables for container, you may use `-e` flag for for `docker run` but we advice you to set all environment variables in a file and use  `--env-file` flag when starting your container.

Then you can share same file among the Rakam containers. If Dockerfile can't find any environment variable starts with `RAKAM_`, it tries to connect Postgresql instance created with docker-compose.

### AWS (Cloudformation)

Cloudformation templates create a Opsworks stack in your AWS account for Rakam. You can easily monitor, scale and manage your Rakam cluster with with this Cloudformation templates.

Cloudformation is the recommended way to deploy Rakam in production but unfortunately they're not open-source yet. Please request Cloudformation template so that [we can help you to deploy](https://rakam.io/contact/?topic=Request for Cloudformation template) Rakam in our AWS account with Cloudformation.

### Custom

Download latest version from [Bintray](https://dl.bintray.com/buremba/maven/org/rakam/rakam/0.5/rakam-0.5-bundle.tar.gz), extract package, modify `etc/config.properties` file and run `bin/launcher start`.
The launcher script can take the following arguments: `start|restart|stop|status|run`. 
`bin/launcher run` will start Rakam in foreground.

### Managed

We're also working for managed Rakam cluster, we will deploy Rakam to our AWS accounts and manage it for you so that you don't need to worry about scaling, managing and software updates. We will do it for you.
Please shoot us an email to `support@rakam.io` if you want to test our managed Rakam service.

Web application
------------
This repository contains Rakam API server that allows you to interact with Rakam using a REST interface. If you already have a frontend and developed a custom analytics service based on Rakam, it's all you need.

However, we also developed Rakam Web Application that allows you to analyze your user and event data-set but performing SQL queries, visualising your data in various charts, creating (real-time) dashboards and custom reports. You can turn Rakam into a analytics web service similar to [Mixpanel](https://mixpanel.com), [Kissmetrics](https://kissmetrics.com) and [Localytics](https://localytics.com) using the web application. Otherwise, Rakam server is similar to [Keen.io](http://keen.io) with SQL as query language and some extra features.

Another nice property of Rakam web application is being BI `(Business Intelligence)` tool. If you can disable collect APIs and connect Rakam to your SQL database with JDBC adapter and use Rakam application to query your data in your database. Rakam Web Application has various charting formats, supports parameterized SQL queries, custom pages that allows you to design pages with internal components.

Contribution
------------
Currently I'm actively working on Rakam. If you want to contribute the project or suggest an idea feel free to fork it or create a ticket for your suggestion. I promise to respond you ASAP.
The purpose of Rakam is being generic data analysis tool that can be a solution for many use cases. Rakam still needs too much work and will be evolved based on people's needs so your thoughts are important.
