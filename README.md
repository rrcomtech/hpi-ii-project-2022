# HPI information integration project SoSe 2022
by Robert Richter and Fabian Lange

This project integrates the given data source "Handelsregisterbekanntmachungen" with the data set "Voting Rights Proportions" from Bafin.

## Architecture

![](architecture_new.png)

## How to run the system

Start the initial project setup as explained below in the initial project readme. Alternatively, you can use the shared dataset when storing it to the folder `rb_crawler` and running 

```
poetry run python rb_crawler/data_loader.py
```

instead of the given rb_crawler.

Then start the crawling of voting rights proportions:

Crawl the "Voting Rights Proportions" dataset in two steps:
1. Crawl the general data for each letter of the alphabet starting with letter  `A`  including a bafin-id, name, domicile and country for each company.
2. Crawl detail data for each company that was returned during the first step including data for each issuer of a company: bafin-id, name, domicile, country, publishing data and their voting rights split up into three categories of german law paragraphs: [1] §33 and §34, [2] §38 and [3] §39.

The second step will only be executed when crawling in detail mode. This can be indicated when starting the crawler with the command below:

```shell
poetry run python rights_bafin_crawler/main.py --detail "True"
# --detail : True if crawling also the details for each company, False if not.
```

The crawler automatically produces results using [protobuf schemata](./proto/bakdata/bafin/v1/) by doing the following requests on the bafin website:

```shell
# General search for every letter starting with A
export LETTER = "A"
curl -X Get "https://portal.mvp.bafin.de/database/AnteileInfo/suche.do?nameAktiengesellschaftButton=Suche+Emittent&d-7004401-e=1&6578706f7274=1&nameMeldepflichtiger=&nameAktiengesellschaft=$LETTER"
```

```shell
# Detail search for every result from the general search if in detail mode
export BAFIN_ID = "40001244"
curl -X Get "https://portal.mvp.bafin.de/database/AnteileInfo/aktiengesellschaft.do?d-3611442-e=1&cmd=zeigeAktiengesellschaft&id=$BAFIN_ID&6578706f7274=1"
```

For producing intersecting entries (e.g. those with the same company) of the crawling results start the unionizer with 

```
poetry run python union/main.py
```

The results of the union process will be produced to another union schema in kafka.

For the detection of duplicate persons, please start 

```
poetry run python person_dedup/main.py
```

The persons will then be inspected for duplicates. All distinct persons will be produced to the topic persons_dedup in kafka and all duplicate persons will be produced to dup_persons. 

The visualization of results is done with Kibana which we included in our docker configuration. To see the visualized results, please visit its GUI at https://localhost:5601 when running the project and navigate to the preferred chart.

# Initial Repository Readme
This repository provides a code base for the information integration lecture in the summer semester of 2022. Below you
can find the documentation for setting up the project.

## Prerequisites

- Install [Poetry](https://python-poetry.org/docs/#installation)
- Install [Docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/)
- Install [Protobuf compiler (protoc)](https://grpc.io/docs/protoc-installation/). If you are using windows you can
  use [this guide](https://www.geeksforgeeks.org/how-to-install-protocol-buffers-on-windows/)
- Install [jq](https://stedolan.github.io/jq/download/)

### RB Website

The [Registerbekanntmachung website](https://www.handelsregisterbekanntmachungen.de/index.php?aktion=suche) contains
announcements concerning entries made into the companies, cooperatives, and
partnerships registers within the electronic information and communication system. You can search for the announcements.
Each announcement can be requested through the link below. You only need to pass the query parameters `rb_id`
and `land_abk`. For instance, we chose the state Rheinland-Pfalz `rp` with an announcement id of `56267`, the
new entry of the company BioNTech.

```shell
export STATE="rp" 
export RB_ID="56267"
curl -X GET  "https://www.handelsregisterbekanntmachungen.de/skripte/hrb.php?rb_id=$RB_ID&land_abk=$STATE"
```

### RB Crawler

The Registerbekanntmachung crawler (rb_crawler) sends a get request to the link above with parameters (`rb_id`
and `land_abk`) passed to it and extracts the information from the response.

We use [Protocol buffers](https://developers.google.com/protocol-buffers)
to define our [schema](./proto/bakdata/corporate/v1/corporate.proto).

The crawler uses the generated model class (i.e., `Corporate` class) from
the [protobuf schema](./proto/bakdata/corporate/v1/corporate.proto).
We will explain furthur how you can generate this class using the protobuf compiler.
The compiler creates a `Corporate` class with the fields defined in the schema. The crawler fills the object fields with
the
extracted data from the website.
It then serializes the `Corporate` object to bytes so that Kafka can read it and produces it to the `corporate-events`
topic. After that, it increments the `rb_id` value and sends another GET request.
This process continues until the end of the announcements is reached, and the crawler will stop automatically.

### corporate-events topic

The `corporate-events` holds all the events (announcements) produced by the `rb_crawler`. Each message in a Kafka topic
consist of a key and value.

The key type of this topic is `String`. The key is generated by the `rb_crawler`. The key
is a combination of the `land_abk` and the `rb_id`. If we consider the `rb_id` and `land_abk` from the example above,
the
key will look like this: `rp_56267`.

The value of the message contains more information like `event_name`, `event_date`, and more. Therefore, the value type
is complex and needs a schema definition.

### Kafka Connect

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) is a tool to move large data sets into
(source) and out (sink) of Kafka.
Here we only use the Sink connector, which consumes data from a Kafka topic into a secondary index such as
Elasticsearch.

We use the [Elasticsearch Sink Connector](https://docs.confluent.io/kafka-connect-elasticsearch/current/overview.html)
to move the data from the `coporate-events` topic into the Elasticsearch.

## Setup

This project uses [Poetry](https://python-poetry.org/) as a build tool.
To install all the dependencies, just run `poetry install`.

This project uses Protobuf for serializing and deserializing objects. We provided a
simple [protobuf schema](./proto/bakdata/corporate/v1/corporate.proto).
Furthermore, you need to generate the Python code for the model class from the proto file.
To do so run the [`generate-proto.sh`](./generate-proto.sh) script.
This script uses the [Protobuf compiler (protoc)](https://grpc.io/docs/protoc-installation/) to generate the model class
under the `build/gen/bakdata/corporate/v1` folder
with the name `corporate_pb2.py`.

## Run

### Infrastructure

Use `docker-compose up -d` to start all the services: [Zookeeper](https://zookeeper.apache.org/)
, [Kafka](https://kafka.apache.org/), [Schema
Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
, [Kafka REST Proxy]((https://github.com/confluentinc/kafka-rest)), [Kowl](https://github.com/redpanda-data/kowl),
[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html),
and [Elasticsearch](https://www.elastic.co/elasticsearch/). Depending on your system, it takes a couple of minutes
before the services are up and running. You can use a tool
like [lazydocker](https://github.com/jesseduffield/lazydocker)
to check the status of the services.

### Kafka Connect

After all the services are up and running, you need to configure Kafka Connect to use the Elasticsearch sink connector.
The config file is a JSON formatted file. We provided a [basic configuration file](./connect/elastic-sink.json).
You can find more information about the configuration properties on
the [official documentation page](https://docs.confluent.io/kafka-connect-elasticsearch/current/overview.html).

To start the connector, you need to push the JSON config file to Kafka. You can either use the UI dashboard in Kowl or
use the [bash script provided](./connect/push-config.sh). It is possible to remove a connector by deleting it
through Kowl's UI dashboard or calling the deletion API in the [bash script provided](./connect/delete-config.sh).

### RB Crawler

You can start the crawler with the command below:

```shell
poetry run python rb_crawler/main.py --id $RB_ID --state $STATE
```

The `--id` option is an integer, which determines the initial event in the handelsregisterbekanntmachungen to be
crawled.

The `--state` option takes a string (only the ones listed above). This string defines the state where the crawler should
start from.

You can use the `--help` option to see the usage:

```
Usage: main.py [OPTIONS]

Options:
  -i, --id INTEGER                The rb_id to initialize the crawl from
  -s, --state [bw|by|be|br|hb|hh|he|mv|ni|nw|rp|sl|sn|st|sh|th]
                                  The state ISO code
  --help                          Show this message and exit.
```

## Query data

### Kowl

[Kowl](https://github.com/redpanda-data/kowl) is a web application that helps you manage and debug your Kafka workloads
effortlessly. You can create, update, and delete Kafka resources like Topics and Kafka Connect configs.
You can see Kowl's dashboard in your browser under http://localhost:8080.

### Elasticsearch

To query the data from Elasticsearch, you can use
the [query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl.html) of elastic. For example:

```shell
curl -X GET "localhost:9200/_search?pretty" -H 'Content-Type: application/json' -d'
{
    "query": {
        "match": {
            <field>
        }
    }
}
'
```

`<field>` is the field you wish to search. For example:

```
"reference_id":"HRB 41865"
```

## Teardown
You can stop and remove all the resources by running:
```shell
docker-compose down
```
