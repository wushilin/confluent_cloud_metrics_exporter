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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class ConfluentCloudMetricsSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(ConfluentCloudMetricsSourceConnector.class);

    public static final String TOPIC_CONFIG = "topic";
    public static final String API_KEY = "apikey";
    public static final String API_SECRET = "apisecret";
    public static final String KAFKA_CLUSTER_IDS = "cluster_ids";
    public static final String PROXY = "proxy";
    public static final String PROXY_PORT = "proxyPort";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(API_KEY, Type.STRING, "", Importance.HIGH, "Confluent Cloud API Key")
            .define(API_SECRET, Type.PASSWORD, "", Importance.HIGH, "Confluent Cloud API Secret")
            .define(KAFKA_CLUSTER_IDS, Type.LIST, Importance.HIGH, "Comma separated Confluent Cloud Cluster IDs")
            .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to")
            .define(PROXY, Type.STRING, "", Importance.LOW, "Proxy Server to use")
            .define(PROXY_PORT, Type.STRING, "", Importance.LOW, "Proxy Server Port to use");

    private String topic;
    private String apiKey;
    private Password apiSecret;
    private List<String> kafkaClusterIds;
    private int batchSize;
    private String proxy;
    private String proxyPort;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting...");
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        kafkaClusterIds = new ArrayList<>();
        List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in ConfluentCloudMetricsSourceConnector configuration requires definition of a single topic");
        }
        List<String> topicsFromConfig = parsedConfig.getList(KAFKA_CLUSTER_IDS);
        kafkaClusterIds.addAll(topicsFromConfig);
        topic = topics.get(0);
        apiKey = parsedConfig.getString(API_KEY);
        apiSecret = parsedConfig.getPassword(API_SECRET);
        proxy = parsedConfig.getString(PROXY);
        proxyPort = parsedConfig.getString(PROXY_PORT);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ConfluentCloudMetricsSourceTask.class;
    }

    private static String concat(List<String> input) {
        if(input == null) {
            return "";
        }

        if(input.size() == 0) {
            return "";
        }
        return String.join(",", input);
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(API_KEY, apiKey);
        config.put(API_SECRET, apiSecret.value());
        config.put(KAFKA_CLUSTER_IDS, concat(kafkaClusterIds));
        config.put(PROXY, proxy);
        config.put(PROXY_PORT, proxyPort);
        configs.add(config);
        Map<String, String> debugConfig = new HashMap<String, String>();
        debugConfig.putAll(config);
        debugConfig.put(API_SECRET, "******");
        log.info("TaskConfigs: " + debugConfig);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
        log.info("Stopped.");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}