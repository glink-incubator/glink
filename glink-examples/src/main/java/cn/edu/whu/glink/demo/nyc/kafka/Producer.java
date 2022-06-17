package cn.edu.whu.glink.demo.nyc.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * @author Xu Qi
 */
public class Producer implements Callable<Integer> {

  private final BlockingQueue<String> queue;
  private int count = 0;
  private int pid;
  private final int threadId;

  private final String topic;
  private final KafkaProducer<Integer, String> kafkaProducer;

  public Producer(final BlockingQueue<String> queue,
                  final String topic,
                  final String bootstrapServer,
                  final int threadId,
                  final int pid) {
    this.queue = queue;
    this.topic = topic;
    this.threadId = threadId;
    this.pid = pid;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProducer = new KafkaProducer<>(props);
  }

  @Override
  public Integer call() throws Exception {
    while (true) {
      String line = queue.take();
      if ("null".equals(line)) {
        break;
      }

      // add write timestamp to the end of the line
      line = pid + "," + line + "," + System.currentTimeMillis();
      pid++;

      // send line to kafka
      kafkaProducer.send(new ProducerRecord<>(topic, null, line));

      ++count;
      if (count % 1000 == 0) {
        System.out.printf("Thread %d produced %d records\n", threadId, count);
      }
    }
    return count;
  }
}
