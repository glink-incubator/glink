package cn.edu.whu.glink.demo.nyc.kafka;

import cn.edu.whu.glink.core.tile.PixelResult;
import cn.edu.whu.glink.core.tile.TileResult;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.DataOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * @author Xu Qi
 */
public class NycHeatmapSerializer implements KafkaSerializationSchema<TileResult<Double>> {
  private final String topic;

  public static final String AVRO_SCHEMA = "{"
      + "\"type\": \"record\" ,"
      + "  \"name\": \"TileResult\","
      + "  \"fields\": [\n"
      + "    {\"name\": \"pixNos\", \"type\": { \"type\": \"array\", \"items\": \"int\"}},\n"
      + "    {\"name\": \"values\", \"type\": { \"type\": \"array\", \"items\": \"int\"}}\n"
      + "]}";
  private static Schema schema;
  protected static Injection<GenericRecord, byte[]> injection;

  public NycHeatmapSerializer(String topic) {
    this.topic = topic;
  }

  @Override
  public void open(SerializationSchema.InitializationContext context) throws Exception {
    schema = new Schema.Parser().parse(AVRO_SCHEMA);
    injection = GenericAvroCodecs.toBinary(schema);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(TileResult t, @Nullable Long timestamp) {
    resultListToOutputFormat(t);
    StringBuilder sb = new StringBuilder();
    sb.append(t.getTile().toLong()).append(",");
    sb.append(t.getTimeStart()).append(",");
    sb.append(t.getTimeEnd()).append(",");
    sb.append(t.getData()).append(",");
    sb.append(System.currentTimeMillis());
    return new ProducerRecord<>(topic, null, sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  public void resultListToOutputFormat(TileResult record) {
    record.setData(Base64.getEncoder().encodeToString(serializedata(record)));
  }

  private byte[] serializedata(TileResult record) {
    List<Integer> pixNos = new ArrayList<>();
    List<Double> values = new ArrayList<>();
    int i = 0;
    for (Object pixelResult : record.getGridResultList()) {
      pixNos.add(((PixelResult) pixelResult).getPixel().getPixelNo());
      values.add((Double) ((PixelResult) pixelResult).getResult());
    }
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("pixNos", pixNos);
    genericRecord.put("values", values);
    return injection.apply(genericRecord);
  }
}
