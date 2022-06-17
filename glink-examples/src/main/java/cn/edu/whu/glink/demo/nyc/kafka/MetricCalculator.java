package cn.edu.whu.glink.demo.nyc.kafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Xu Qi
 */
public class MetricCalculator {
  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("bootstrapServer", "s", true, "kafka broker");
    options.addOption("tileTopic", "t", true, "The tile topic to read");
    options.addOption("pointTopic", "p", true, "The point topic to read");
    options.addOption("groupId", "g", true, "group id");

    CommandLine cliParser = new DefaultParser().parse(options, args);

//    String bootstrapServer = cliParser.getOptionValue("bootstrapServer");
//    String topic = cliParser.getOptionValue("topic");
//    String groupId = cliParser.getOptionValue("groupId");

    String bootstrapServer = "172.21.184.80:9092";
    String tileTopic = "nyc_throughput_out";
    String pointTopic = "nyc_throughput_in";
    String groupId = "ntdata";
    String groupId2 = "npdata";

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(tileTopic));
    int totalRecords = 0;
    double totalLatency = 0;
    long minInTime = Long.MAX_VALUE, maxInTime = Long.MIN_VALUE;
    long minOutTime = Long.MAX_VALUE, maxOutTime = Long.MIN_VALUE;

    while (true) {
      ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(10));
      if (records.isEmpty()) {
        System.out.println("empty");
        break;
      }
      for (ConsumerRecord<Integer, String> record : records) {
        String line = record.value();
        String[] items = line.split(",");
        long outTime = 0;
        outTime = Long.parseLong(items[items.length - 1]);
        minOutTime = Math.min(minOutTime, outTime);
        maxOutTime = Math.max(maxOutTime, outTime);
      }
    }

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId2);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<Integer, String> consumer2 = new KafkaConsumer<>(properties);
    consumer2.subscribe(Collections.singletonList(pointTopic));
    while (true) {
      ConsumerRecords<Integer, String> records = consumer2.poll(Duration.ofSeconds(10));
      if (records.isEmpty()) {
        break;
      }
      for (ConsumerRecord<Integer, String> record : records) {
        ++totalRecords;
      }
    }

    System.out.println("Total records: " + totalRecords);
//    System.out.printf("Input time range: [%d, %d]\n", minInTime, maxInTime);
    System.out.printf("Output time range: [%d, %d]\n", minOutTime, maxOutTime);
//    System.out.printf("Latency: %f ms\n", (totalLatency / totalRecords));
    System.out.printf("Throughput: %f r/s\n", (((double) totalRecords) / ((maxOutTime - minOutTime) / 1000)));
  }
}
