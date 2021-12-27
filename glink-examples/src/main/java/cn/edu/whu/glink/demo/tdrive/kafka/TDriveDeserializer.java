package cn.edu.whu.glink.demo.tdrive.kafka;

import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author Yu Liebing
 * */
public class TDriveDeserializer implements KafkaDeserializationSchema<Point2> {

  private boolean withPid = false;
  private transient DateTimeFormatter formatter;

  public TDriveDeserializer() {
    this(false);
  }

  public TDriveDeserializer(boolean withPid) {
    this.withPid = withPid;
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public boolean isEndOfStream(Point2 point2) {
    return false;
  }

  @Override
  public Point2 deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    String line = new String(consumerRecord.value(), StandardCharsets.UTF_8);
    String[] items = line.split(",");
    if (!withPid) {
      String id = items[0];
      long timestamp = LocalDateTime.parse(items[1], formatter)
              .toInstant(ZoneOffset.of("+8")).toEpochMilli();
      double lng = Double.parseDouble(items[2]);
      double lat = Double.parseDouble(items[3]);
      long inputTime = Long.parseLong(items[4]);
      Point2 point = new Point2(id, timestamp, lng, lat);
      point.setUserData(new Tuple1<>(inputTime));
      return point;
    } else {
      String pid = items[0];
      String cid = items[1];
      long timestamp = LocalDateTime.parse(items[2], formatter)
              .toInstant(ZoneOffset.of("+8")).toEpochMilli();
      double lng = Double.parseDouble(items[3]);
      double lat = Double.parseDouble(items[4]);
      long inputTime = Long.parseLong(items[5]);
      Point2 point = new Point2(pid, timestamp, lng, lat);
      point.setUserData(new Tuple2<>(cid, inputTime));
      return point;
    }
  }

  @Override
  public TypeInformation<Point2> getProducedType() {
    return TypeInformation.of(Point2.class);
  }
}
