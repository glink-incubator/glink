package cn.edu.whu.glink.demo.tdrive.kafka;

import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.locationtech.jts.geom.Geometry;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author Yu Liebing
 * */
public class TDriveDimJoinSerializer implements KafkaSerializationSchema<Tuple2<Point2, Geometry>> {

  private final String topic;

  public TDriveDimJoinSerializer(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(Tuple2<Point2, Geometry> t, @Nullable Long timestamp) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.f0.getId()).append(",");
    sb.append(t.f0.getTimestamp()).append(",");
    sb.append(t.f0.getX()).append(",");
    sb.append(t.f0.getY()).append(",");
    Tuple1<Long> pUserData = (Tuple1<Long>) t.f0.getUserData();
    sb.append(pUserData.f0).append(",");
    if (t.f1 != null) {
      Tuple2<String, String> userData = (Tuple2<String, String>) t.f1.getUserData();
      sb.append(userData.f0).append(",");
      sb.append(userData.f1).append(",");
    }
    sb.append(System.currentTimeMillis());
    return new ProducerRecord<>(topic, sb.toString().getBytes(StandardCharsets.UTF_8));
  }
}
