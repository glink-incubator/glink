package cn.edu.whu.glink.demo.nyc.kafka;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * @author Xu Qi
 */
public class NycDeserializer implements KafkaDeserializationSchema<Tuple2<Point, String>> {

  private boolean withPid = false;
  private transient DateTimeFormatter formatter;
  private GeometryFactory factory;

  public NycDeserializer() {
    this(false);
  }

  public NycDeserializer(boolean withPid) {
    this.withPid = withPid;
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    factory = SpatialDataStream.geometryFactory;
  }

  @Override
  public boolean isEndOfStream(Tuple2<Point, String> pointStringTuple2) {
    return false;
  }

  @Override
  public Tuple2<Point, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    String line = new String(consumerRecord.value(), StandardCharsets.UTF_8);
    String[] items = line.split(",");
    if (!withPid) {
      String id = items[0];
//      long timestamp = LocalDateTime.parse(items[1], formatter)
//          .toInstant(ZoneOffset.of("+8")).toEpochMilli();
//      BigDecimal db = new BigDecimal(items[1]);
//      long timestamp = Long.parseLong(db.toPlainString());
      long timestamp = Long.parseLong(items[1].substring(0, 13));
      double lng = Double.parseDouble(items[2]);
      double lat = Double.parseDouble(items[3]);
      Point point = factory.createPoint(new Coordinate(lng, lat));
      Tuple attributes = Tuple.newInstance(items.length - 2);
      for (int i = 0, j = 0; i < items.length; i++) {
        if (i == 0 || i == 1 || i > 3) {
          if (i == 0) {
            attributes.setField(id, j);
          }
          if (i == 1) {
            attributes.setField(timestamp, j);
          } else {
            attributes.setField(items[i], j);
          }
          j++;
        }
      }
      point.setUserData(attributes);
      return new Tuple2<>(point, id);
    } else {
      String pid = items[0];
      String id = items[1];
      long timestamp = LocalDateTime.parse(items[2], formatter)
          .toInstant(ZoneOffset.of("+8")).toEpochMilli();
      double lng = Double.parseDouble(items[3]);
      double lat = Double.parseDouble(items[4]);
      Point point = factory.createPoint(new Coordinate(lng, lat));
      Tuple attributes = Tuple.newInstance(items.length - 2);
      for (int i = 0, j = 0; i < items.length; i++) {
        if (i < 3 || i > 4) {
          if (i == 0) {
            attributes.setField(pid, j);
          }
          if (i == 1) {
            attributes.setField(id, j);
          }
          if (i == 2) {
            attributes.setField(timestamp, j);
          } else {
            attributes.setField(items[i], j);
          }
          j++;
        }
      }
      point.setUserData(attributes);
      return new Tuple2<>(point, id);
    }
  }

  @Override
  public TypeInformation<Tuple2<Point, String>> getProducedType() {
    return TypeInformation.of(new TypeHint<Tuple2<Point,  String>>() { });
  }
}
