package cn.edu.whu.glink.demo.tdrive.kafka;

import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author Yu Liebing
 * */
public class TDriveSerializer implements KafkaSerializationSchema<Point2> {

  private final String topic;
  private boolean isCluster = false;

  public TDriveSerializer(String topic) {
    this.topic = topic;
  }

  public TDriveSerializer(String topic, boolean isCluster) {
    this.topic = topic;
    this.isCluster = isCluster;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Point2 point2, @Nullable Long timestamp) {
    StringBuilder sb = new StringBuilder();
    if (!isCluster) {
      sb.append(point2.getId()).append(",");
      sb.append(point2.getTimestamp()).append(",");
      sb.append(point2.getX()).append(",");
      sb.append(point2.getY()).append(",");
      Tuple1<Long> userData = (Tuple1<Long>) point2.getUserData();
      sb.append(userData.f0).append(",");
      // add the output timestamp
      sb.append(System.currentTimeMillis());
    } else {
      Tuple3<String, Long, Integer> userData = (Tuple3<String, Long, Integer>) point2.getUserData();
      sb.append(point2.getId()).append(",");
      sb.append(userData.f0).append(",");
      sb.append(point2.getTimestamp()).append(",");
      sb.append(point2.getX()).append(",");
      sb.append(point2.getY()).append(",");
      sb.append(userData.f1).append(",");
      // add the output timestamp
      sb.append(System.currentTimeMillis()).append(",");
      sb.append(userData.f2);
    }
    return new ProducerRecord<>(topic, null, sb.toString().getBytes(StandardCharsets.UTF_8));
  }
}
