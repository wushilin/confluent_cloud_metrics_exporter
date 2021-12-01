package net.wushilin.ccloud.metrics.connector; /**
 * Copyright 2020 Oli Watson
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class MetricsAPISchemas {
    public static String SCHEMA_KEY = "metric.key";
    public static String SCHEMA_METRIC_VALUE = "metric.value";

    public static final String METRIC_NAME = "name";
    public static final String CLUSTER_ID = "cluster_id";
    public static final String TOPIC_NAME = "topic";
    public static final String VALUE = "value";
    public static final String TYPE = "type";
    public static final String TIMESTAMP = "timestamp";

    // Key Schema
    public static Schema KEY_SCHEMA = SchemaBuilder.struct().name(SCHEMA_KEY)
            .field(METRIC_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CLUSTER_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TOPIC_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TYPE, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    // Value Schema
    public static Schema METRIC_VALUE_SCHEMA = SchemaBuilder.struct().name(SCHEMA_METRIC_VALUE)
            .field(METRIC_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CLUSTER_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TOPIC_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(VALUE, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
            .field(TYPE, Schema.OPTIONAL_STRING_SCHEMA)
            .build();
}
