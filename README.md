# confluent_cloud_metrics_exporter
This connector imports your Confluent Metrics to your Kafka Cluster using Kafka Connect.

# Building
```bash
user@host $ ./gradlew jar

Locate the built jar at ./build/libs/ConfluentCloudMetricsSourceConnector-1.0-SNAPSHOT.jar

```

# Installing
1. Copy the jar to a folder like /opt/connectors/ccloud_metrics_exporter/ConfluentCloudMetricsSourceConnector-1.0-SNAPSHOT.jar
2. Make Sure /opt/connectors is in your Kafka Connect plugin path. 
3. You have to restart your Kafka Connect after that.

If you prefer to use existing path, you can copy it to your default plugin path. Make sure you create a directory for this plugin.


#Required configuration:
```
topic: Which topic to write to
apikey: The Confluent Cloud API Key
apisecret: The Confluent Cloud API Key Secret
cluster_ids: List of cluster IDs to monitor
proxy: Optional http proxy host
proxyPort: Optional http proxy port
```

# Schema
The data is in Avro format. The scehma is as follows
Message Key:
```json
{
  "connect.name": "metric.key",
  "fields": [
    {
      "default": null,
      "name": "name",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "cluster_id",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "topic",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "type",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "timestamp",
      "type": [
        "null",
        "long"
      ]
    }
  ],
  "name": "key",
  "namespace": "metric",
  "type": "record"
}
```

Message Value:
```json
{
  "connect.name": "metric.value",
  "fields": [
    {
      "default": null,
      "name": "name",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "cluster_id",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "topic",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "value",
      "type": [
        "null",
        "double"
      ]
    },
    {
      "default": null,
      "name": "timestamp",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "default": null,
      "name": "type",
      "type": [
        "null",
        "string"
      ]
    }
  ],
  "name": "value",
  "namespace": "metric",
  "type": "record"
}
```
