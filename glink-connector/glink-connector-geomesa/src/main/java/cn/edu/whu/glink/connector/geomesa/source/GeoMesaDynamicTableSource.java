package cn.edu.whu.glink.connector.geomesa.source;

import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * GeoMesa table source implementation.
 *
 * @author Yu Liebing
 * */
public class GeoMesaDynamicTableSource implements
        ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

  private final GeoMesaDataStoreParam param;
  private final GeoMesaTableSchema schema;

  public GeoMesaDynamicTableSource(GeoMesaDataStoreParam param, GeoMesaTableSchema schema) {
    this.param = param;
    this.schema = schema;
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    int[][] keys = lookupContext.getKeys();
    checkArgument(
            keys.length == 1 && keys[0].length == 1,
            "Currently, GeoMesa table can only be lookup by single geometry field.");
    String queryField = schema.getFieldName(keys[0][0]);
    GeoMesaRowConverter<RowData> geoMesaRowConverter =
            new SimpleFeatureToRowDataConverter(schema);
    return TableFunctionProvider.of(
            new GeoMesaRowDataLookupFunction(param, schema, geoMesaRowConverter, queryField));
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    GeoMesaRowConverter<RowData> geoMesaRowConverter =
            new SimpleFeatureToRowDataConverter(schema);
    GeoMesaSourceFunction<RowData> sourceFunction =
            new GeoMesaSourceFunction<>(param, schema, geoMesaRowConverter);
    return SourceFunctionProvider.of(sourceFunction, true);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public DynamicTableSource copy() {
    return null;
  }

  @Override
  public String asSummaryString() {
    return "GeoMesa";
  }

  @Override
  public boolean supportsNestedProjection() {
    // planner doesn't support nested projection push down yet.
    return false;
  }

  @Override
  public void applyProjection(int[][] ints) {

  }
}
