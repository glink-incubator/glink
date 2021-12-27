package cn.edu.whu.glink.demo.tdrive.kafka;

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
 * @author Yu Liebing
 * */
public class MetricCalculator {

  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("bootstrapServer", "s", true, "kafka broker");
    options.addOption("topic", "t", true, "The topic to read");
    options.addOption("groupId", "g", true, "group id");
    options.addOption("mode", "m", true, "Which kind of output to calculate,"
            + "single - to calculate the output of filter and knn, "
            + "join - to calculate the out put of window/interval join, "
            + "dim - to calculate the output of dimension join, "
            + "dbscan- to calculate the output of dbscan");
    CommandLine cliParser = new DefaultParser().parse(options, args);

    String bootstrapServer = cliParser.getOptionValue("bootstrapServer");
    String topic = cliParser.getOptionValue("topic");
    String groupId = cliParser.getOptionValue("groupId");
    String mode = cliParser.getOptionValue("mode");

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    int totalRecords = 0;
    double totalLatency = 0.;
    long minInTime = Long.MAX_VALUE, maxInTime = Long.MIN_VALUE;
    long minOutTime = Long.MAX_VALUE, maxOutTime = Long.MIN_VALUE;

    while (true) {
      ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(10));
      if (records.isEmpty()) {
        break;
      }
      for (ConsumerRecord<Integer, String> record : records) {
        String line = record.value();
        String[] items = line.split(",");
        long inTime = 0;
        long outTime = 0;
        if ("single".equals(mode)) {
          inTime = Long.parseLong(items[4]);
          outTime = Long.parseLong(items[5]);
        } else if ("join".equals(mode)) {
          long t1 = Long.parseLong(items[4]);
          long t2 = Long.parseLong(items[9]);
          inTime = Math.max(t1, t2);
          outTime = Long.parseLong(items[10]);
        } else if ("dim".equals(mode)) {
          inTime = Long.parseLong(items[4]);
          outTime = Long.parseLong(items[items.length - 1]);
        } else if ("dbscan".equals(mode)) {
          inTime = Long.parseLong(items[5]);
          outTime = Long.parseLong(items[6]);
        }
        totalLatency += (outTime - inTime);
        minInTime = Math.min(minInTime, inTime);
        maxInTime = Math.max(maxInTime, inTime);
        minOutTime = Math.min(minOutTime, outTime);
        maxOutTime = Math.max(maxOutTime, outTime);
        ++totalRecords;
      }
    }

    System.out.println("Total records: " + totalRecords);
    System.out.printf("Input time range: [%d, %d]\n", minInTime, maxInTime);
    System.out.printf("Output time range: [%d, %d]\n", minOutTime, maxOutTime);
    System.out.printf("Latency: %f ms\n", (totalLatency / totalRecords));
    System.out.printf("Throughput: %f r/s\n", (((double) totalRecords) / ((maxOutTime - minOutTime) / 1000)));
  }
}
