/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.wushilin.ccloud.metrics.connector;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.net.http.HttpClient.Version.HTTP_1_1;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class ConfluentCloudMetricsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(ConfluentCloudMetricsSourceTask.class);
    private static final String URL = "https://api.telemetry.confluent.cloud/v2/metrics/{{RESOURCE}}/export";
    private static final String resource = "cloud";
    private String topic = null;
    private List<String> clusterIds = null;
    private String clusterIdsString = null;
    private HttpClient httpClient;
    private String encodedSecret = "";
    private String proxy;
    private String proxyPort;
    private long lastRunMinute = 0;
    public ConfluentCloudMetricsSourceTask() {
    }
    @Override
    public String version() {
        return new ConfluentCloudMetricsSourceConnector().version();
    }

    private List<String> split(String what) {
        return Arrays.asList(what.split(","));
    }

    private static String getUrl(List<String> kafkaIds) throws UnsupportedEncodingException {
        StringBuilder actual = new StringBuilder();
        actual.append(URL.replaceAll("\\{\\{RESOURCE\\}\\}", resource));
        actual.append("?");
        for(String next:kafkaIds) {
            actual.append("resource.kafka.id=").append(URLEncoder.encode(next, "UTF-8")).append("&");
        }
        String actualUrl = actual.toString();
        while(actualUrl.endsWith("&") || actualUrl.endsWith("?")) {
            actualUrl = actualUrl.substring(0, actualUrl.length() - 1);
        }
        return actualUrl;
    }
    @Override
    public void start(Map<String, String> props) {
        topic = props.get(ConfluentCloudMetricsSourceConnector.TOPIC_CONFIG);
        String apiKey = props.get(ConfluentCloudMetricsSourceConnector.API_KEY);
        String apiSecret = props.get(ConfluentCloudMetricsSourceConnector.API_SECRET);
        clusterIds = split(props.get(ConfluentCloudMetricsSourceConnector.KAFKA_CLUSTER_IDS));
        clusterIdsString = props.get(ConfluentCloudMetricsSourceConnector.KAFKA_CLUSTER_IDS);
        proxy = props.get(ConfluentCloudMetricsSourceConnector.PROXY);
        proxyPort = props.get(ConfluentCloudMetricsSourceConnector.PROXY_PORT);
        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(30));
        if(proxy != null && proxy.trim().length() > 0) {
            builder = builder.proxy(ProxySelector.of(new InetSocketAddress(proxy, Integer.parseInt(proxyPort))));
        }
        httpClient = builder.build();
        try {
            Base64.Encoder encoder = Base64.getEncoder();
            encodedSecret = encoder.encodeToString(String.format("%s:%s", apiKey, apiSecret).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private String getBody(List<String> clusterIds) throws IOException, InterruptedException {
        String actualUrl = getUrl(clusterIds);
        log.debug("Actual URL: " + actualUrl);
        HttpRequest outRequest = HttpRequest.newBuilder()
                .version(HTTP_1_1)
                .GET()
                .uri(URI.create(actualUrl))
                .setHeader("Authorization", "Basic " + encodedSecret)
                .build();
        return executeWithRetry(outRequest);

    }

    private String executeWithRetry(HttpRequest request) throws InterruptedException, IOException {
        IOException lastIOE = null;
        for(int i = 0; i < 3; i++) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if(response.statusCode() == 200) {
                    return response.body();
                } else if(response.statusCode() >= 400 && response.statusCode() < 500) {
                    throw new IOException("HTTP " + response.statusCode());
                } else if(response.statusCode() >= 500) {
                    lastIOE = new IOException("Server Error " + response.statusCode());
                    Thread.sleep(1000);
                    continue;
                }
            } catch(IOException ex) {
                lastIOE = ex;
                Thread.sleep(1000);
                continue;
            } catch(InterruptedException ex) {
                throw ex;
            }
        }
        throw lastIOE;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.debug("Polling... Checking pre-requisite");
        while(true) {
            long seconds = System.currentTimeMillis() / 1000;
            long minute = seconds/60;
            long secondsInMinute = seconds % 60;
            if(minute != lastRunMinute && secondsInMinute >= 30 && secondsInMinute <= 50) {
                lastRunMinute = minute;
                break;
            }
            Thread.sleep(100);
        }
        log.debug("Polling records...");
        Map sourcePartition = new HashMap<String,String>();
        sourcePartition.put("SourceCluster", clusterIdsString);

        Map sourceOffset = new HashMap<String,Long>();
        sourceOffset.put("ts", System.currentTimeMillis());
        List<SourceRecord> source = new ArrayList<>();
        try {
            log.debug("Making http request with cluster ids: " + clusterIds);
            String body = getBody(clusterIds);
            log.debug("Response: " + body);
            BufferedReader br = new BufferedReader(new StringReader(body));
            String buffer;
            while((buffer = br.readLine()) != null) {
                if(buffer.startsWith("#")) {
                    continue;
                }
                if(buffer.trim().length() == 0) {
                    continue;
                }
                try {
                    log.debug("Received " + buffer);
                    MetricEntry me = MetricEntry.parse(buffer);
                    log.debug("Converted to " + me);
                    source.add(new SourceRecord(
                            sourcePartition,
                            sourceOffset,
                            topic,
                            null, // partition will be inferred by the framework
                            MetricsAPISchemas.KEY_SCHEMA,
                            buildKey(me),
                            MetricsAPISchemas.METRIC_VALUE_SCHEMA,
                            buildValue(me),
                            me.timestamp));

                } catch(IOException ex) {
                    log.error("Ignoring invalid line: " + buffer, ex);
                }
            }
            log.info("Polled " + source.size() + " records");
        } catch(Exception ex) {
            log.error("Error getting source records. Is the cluster there? " + clusterIds, ex);
        }
        return source;
    }

    private Struct buildValue(MetricEntry me) {
        Struct valuesStruct = new Struct(MetricsAPISchemas.METRIC_VALUE_SCHEMA);
        if(me.clusterId != null) {
            valuesStruct.put(MetricsAPISchemas.CLUSTER_ID, me.clusterId);
        }
        if(me.topicName != null) {
            valuesStruct.put(MetricsAPISchemas.TOPIC_NAME, me.topicName);
        }
        if(me.timestamp != null) {
            valuesStruct.put(MetricsAPISchemas.TIMESTAMP, me.timestamp);
        }

        if(me.metricName != null) {
            valuesStruct.put(MetricsAPISchemas.METRIC_NAME, me.metricName);
        }

        if(me.type != null) {
            valuesStruct.put(MetricsAPISchemas.TYPE, me.type);
        }
        if(me.value != null) {
            valuesStruct.put(MetricsAPISchemas.VALUE, me.value.doubleValue());
        }
        return valuesStruct;


    }
    private Struct buildKey(MetricEntry me) {
        Struct valuesStruct = new Struct(MetricsAPISchemas.KEY_SCHEMA);
        if(me.clusterId != null) {
            valuesStruct.put(MetricsAPISchemas.CLUSTER_ID, me.clusterId);
        }
        if(me.topicName != null) {
            valuesStruct.put(MetricsAPISchemas.TOPIC_NAME, me.topicName);
        }
        if(me.timestamp != null) {
            valuesStruct.put(MetricsAPISchemas.TIMESTAMP, me.timestamp);
        }

        if(me.metricName != null) {
            valuesStruct.put(MetricsAPISchemas.METRIC_NAME, me.metricName);
        }

        if(me.type != null) {
            valuesStruct.put(MetricsAPISchemas.TYPE, me.type);
        }
        return valuesStruct;
    }
    @Override
    public void stop() {
        log.trace("Stopping - NOOP is required.");
    }
}