package cn.edu.whu.glink.connector.geomesa.sink;

import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.table.data.RowData;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

/**
 * An implementation of {@link GeoMesaSimpleFeatureConverter} which converts
 * {@link RowData} into {@link org.opengis.feature.simple.SimpleFeature}.
 *
 * @author Yu Liebing
 */
public class RowDataToSimpleFeatureConverter implements GeoMesaSimpleFeatureConverter<RowData> {

  private final GeoMesaTableSchema geomesaTableSchema;
  private transient SimpleFeatureBuilder builder;

  public RowDataToSimpleFeatureConverter(GeoMesaTableSchema geomesaTableSchema) {
    this.geomesaTableSchema = geomesaTableSchema;
  }

  @Override
  public void open() {
    builder = new SimpleFeatureBuilder(geomesaTableSchema.getSchema());
  }

  @Override
  public SimpleFeature convertToSimpleFeature(RowData record) {
    for (int i = 0, len = record.getArity(); i < len; ++i) {
      builder.set(geomesaTableSchema.getFieldName(i),
              geomesaTableSchema.getFieldEncoder(i).encode(record, i));
    }
    return builder.buildFeature(geomesaTableSchema.getPrimaryKey(record));
  }
}
