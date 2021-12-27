package cn.edu.whu.glink.connector.geomesa.source;

import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.opengis.feature.simple.SimpleFeature;

/**
 * An implementation of {@link GeoMesaRowConverter} which converts {@link org.opengis.feature.simple.SimpleFeature} into
 * {@link RowData}.
 *
 * @author Yu Liebing
 */
public class SimpleFeatureToRowDataConverter implements GeoMesaRowConverter<RowData> {

  private GeoMesaTableSchema tableSchema;

  public SimpleFeatureToRowDataConverter(GeoMesaTableSchema geoMesaTableSchema) {
    this.tableSchema = geoMesaTableSchema;
  }

  @Override
  public void open() { }

  @Override
  public RowData convertToRow(SimpleFeature sf) {
    final int fieldNum = tableSchema.getFieldNum();
    GenericRowData rowData = new GenericRowData(fieldNum);
    for (int i = 0; i < fieldNum; ++i) {
      rowData.setField(i, tableSchema.getFieldDecoder(i).decode(sf, i));
    }
    return rowData;
  }
}
