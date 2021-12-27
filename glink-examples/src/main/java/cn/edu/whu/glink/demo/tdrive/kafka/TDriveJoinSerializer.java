package cn.edu.whu.glink.demo.tdrive.kafka;

import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author Yu Liebing
 * */
public class TDriveJoinSerializer implements KafkaSerializationSchema<Tuple2<Point2, Point2>> {

  private final String topic;

  public TDriveJoinSerializer(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Tuple2<Point2, Point2> record, @Nullable Long timestamp) {
    Point2 p1 = record.f0, p2 = record.f1;
    StringBuilder sb = new StringBuilder();
    // serialize first point
    sb.append(p1.getId()).append(",");
    sb.append(p1.getTimestamp()).append(",");
    sb.append(p1.getX()).append(",");
    sb.append(p1.getY()).append(",");
    Tuple1<Long> d1 = (Tuple1<Long>) p1.getUserData();
    sb.append(d1.f0).append(",");
    // serialize second point
    sb.append(p2.getId()).append(",");
    sb.append(p2.getTimestamp()).append(",");
    sb.append(p2.getX()).append(",");
    sb.append(p2.getY()).append(",");
    Tuple1<Long> d2 = (Tuple1<Long>) p2.getUserData();
    sb.append(d2.f0).append(",");
    // add output timestamp
    sb.append(System.currentTimeMillis());
    return new ProducerRecord<>(topic, null, sb.toString().getBytes(StandardCharsets.UTF_8));
  }
}
