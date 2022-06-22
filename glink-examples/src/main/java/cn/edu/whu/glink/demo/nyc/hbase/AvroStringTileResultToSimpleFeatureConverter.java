package cn.edu.whu.glink.demo.nyc.hbase;

import cn.edu.whu.glink.connector.geomesa.util.AbstractGeoMesaTableSchema;
import cn.edu.whu.glink.core.tile.PixelResult;
import cn.edu.whu.glink.core.tile.TileResult;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * @author Xu Qi
 */
public class AvroStringTileResultToSimpleFeatureConverter implements TileResultToSimpleFeatureConverter {

  private String id;
  private String data;
  private AbstractGeoMesaTableSchema geomesaTableSchema;
  private transient SimpleFeatureBuilder builder;

  public static final String AVRO_SCHEMA = "{"
      + "\"type\": \"record\" ,"
      + "  \"name\": \"TileResult\","
      + "  \"fields\": [\n"
      + "    {\"name\": \"pixNos\", \"type\": { \"type\": \"array\", \"items\": \"int\"}},\n"
      + "    {\"name\": \"values\", \"type\": { \"type\": \"array\", \"items\": \"int\"}}\n"
      + "]}";
  private static Schema schema;
  protected static Injection<GenericRecord, byte[]> injection;

  private static final long serialVersionUID = 6798263106166638810L;

  public AvroStringTileResultToSimpleFeatureConverter(AbstractGeoMesaTableSchema geomesaTableSchema) {
    this.geomesaTableSchema = geomesaTableSchema;
  }

  @Override
  public void open() {
    schema = new Schema.Parser().parse(AVRO_SCHEMA);
    injection = GenericAvroCodecs.toBinary(schema);
    builder = new SimpleFeatureBuilder(geomesaTableSchema.getSimpleFeatureType());
  }

  @Override
  public SimpleFeature convertToSimpleFeature(TileResult record) {
    resultListToOutputFormat(record);
    int cols = geomesaTableSchema.getFieldNum();
    builder.set(0, getPrimaryKey(record));
    builder.set(1, record.getTile().toLong());
    builder.set(2, record.getTimeEnd());
    builder.set(3, record.getData());
    return builder.buildFeature(getPrimaryKey(record));
  }

  @Override
  public void resultListToOutputFormat(TileResult record) {
    record.setData(Base64.getEncoder().encodeToString(serialize(record)));
  }


  public String getPrimaryKey(TileResult tileResult) {
    return fromTimestampToLong(tileResult.getTimeEnd()) + String.valueOf(tileResult.getTile().toLong());
  }


  public void setSimpleFeatureField(int offsetInSchema, TileResult record) {

  }

  private byte[] serialize(TileResult record) {
    List<Integer> pixNos = new ArrayList<>();
    List<Integer> values = new ArrayList<>();
    int i = 0;
    for (Object pixelResult : record.getGridResultList()) {
      pixNos.add(((PixelResult) pixelResult).getPixel().getPixelNo());
      values.add((Integer) ((PixelResult) pixelResult).getResult());
    }
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("pixNos", pixNos);
    genericRecord.put("values", values);
    return injection.apply(genericRecord);
  }

  /**
   * 24  bit length:
   * year - 4 bits - 15 year
   * month- 4 bits - 12 months
   * day  - 5 bits - 31 days
   * hour - 5 bits - 24 hours
   * min  - 6 bits - 60 minutes
   *
   * @param ts
   * @return
   */
  private int fromTimestampToLong(Timestamp ts) {
    LocalDateTime ldt = ts.toLocalDateTime();
    int yearOffset = ldt.getYear() - 2008;
    int monthOffset = ldt.getMonthValue();
    int dayOffset = ldt.getDayOfMonth();
    int hourOffset = ldt.getHour();
    int fiveMinOffset = ldt.getMinute();
    return yearOffset << 20 | monthOffset << 16 | dayOffset << 11 | hourOffset << 6 | fiveMinOffset;
  }
}
