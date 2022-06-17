package cn.edu.whu.glink.demo.nyc.kafka;

import cn.edu.whu.glink.core.tile.Tile;
import cn.edu.whu.glink.demo.nyc.tile.Tile2;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * @author Xu Qi
 */
public class NycHeatmapDeserializer implements KafkaDeserializationSchema<Tile2> {
  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    KafkaDeserializationSchema.super.open(context);
  }

  @Override
  public boolean isEndOfStream(Tile2 tile2) {
    return false;
  }

  @Override
  public Tile2 deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    String line = new String(consumerRecord.value(), StandardCharsets.UTF_8);
    String[] items = line.split(",");
      String tileID = items[0];
      Tile2 tile = new Tile2(tileID);
      Tuple attributes = Tuple.newInstance(items.length);
      for (int i = 0; i < items.length; i++) {
        attributes.setField(items[i], i);
      }
      tile.setUserData(attributes);
      return tile;
  }

  @Override
  public TypeInformation<Tile2> getProducedType() {
    return TypeInformation.of(Tile2.class);
  }
}
