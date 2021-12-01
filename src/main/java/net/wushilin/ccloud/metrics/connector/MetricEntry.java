package net.wushilin.ccloud.metrics.connector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricEntry {
    public String metricName;
    public String clusterId;
    public String topicName;
    public BigDecimal value;
    public Long timestamp;
    public String type;
    public Map<String, String> extendedAttributes = null;

    private static final String regex = "(\\S+)\\{(.+)\\}\\s+(\\S+)\\s+(\\S+)";
    private static final Pattern pattern = Pattern.compile(regex);

    private static final String mapRegex = "([^\"]*)=\"([^\"]*)\",?";
    private static final Pattern mapPattern = Pattern.compile(mapRegex);
    public static MetricEntry parse(String input) throws IOException {
        Matcher m = pattern.matcher(input);
        if(!m.matches()) {
            throw new IOException("Invalid input: " + input);
        }
        MetricEntry me = new MetricEntry();
        me.metricName = m.group(1);
        String extendedAttributesString = m.group(2);
        me.extendedAttributes = parseMap(extendedAttributesString);
        String valueString = m.group(3);
        me.clusterId = me.extendedAttributes.get("kafka_id");
        me.topicName = me.extendedAttributes.get("topic");
        me.type = me.extendedAttributes.get("type");
        String timestampString = m.group(4);
        me.timestamp = Long.parseLong(timestampString);
        me.value = new BigDecimal(valueString);
        return me;
    }

    @Override
    public String toString() {
        return "MetricEntry{" +
                "metricName='" + metricName + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", topicName='" + topicName + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", type='" + type + '\'' +
                ", extendedAttributes=" + extendedAttributes +
                '}';
    }

    public static void main(String[] args) throws IOException {
        try(BufferedReader br = new BufferedReader(new FileReader("test.bin"))) {
            br.lines().forEach((line) -> {
                try {
                    System.out.println(parse(line));
                } catch(Exception ex) {

                }
            });
        }
    }
    // kafka_id="lkc-zdk6z",type="ApiVersions",
    private static Map<String, String> parseMap(String input) {
        Map<String, String> result = new HashMap<String, String>();
        Matcher m = mapPattern.matcher(input);
        while(m.find()) {
            String key = m.group(1);
            String value = m.group(2);
            result.put(key, value);
        }
        return result;
    }
}
