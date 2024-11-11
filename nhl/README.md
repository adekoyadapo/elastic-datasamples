# NHL Play by Play -> Elasticsearch

## Version Requirements

The example has been tested against the following versions:

- Elasticsearch 8.0.0
- Logstash 5.0.0
- Kibana 6.0.0
- Python

## Datasets

Datasets are collected from http://live.nhl.com.

Imports it into Elasticsearch by season or by game.

## Installation & Setup

- Follow the [Installation & Setup Guide](https://github.com/elastic/examples/blob/master/Installation%20and%20Setup.md) to install and test the Elastic Stack (*you can skip this step if you have a working installation of the Elastic Stack,*)

- Check that Elasticsearch and Kibana are up and running.
  - Open `localhost:9200` in web browser -- should return status code 200
  - Open `localhost:5601` in web browser -- should display Kibana UI.

**Note:** By default, Elasticsearch runs on port 9200, and Kibana run on ports 5601. If you changed the default ports, change   the above calls to use appropriate ports.
