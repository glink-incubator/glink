package cn.edu.whu.glink.connector.geomesa.sink;

import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * GeoMesa table sink implementation.
 *
 * @author Yu Liebing
 * */
@Internal
public class GeoMesaDynamicTableSink implements DynamicTableSink {

  private final GeoMesaDataStoreParam param;
  private final GeoMesaTableSchema geomesaTableSchema;

  public GeoMesaDynamicTableSink(GeoMesaDataStoreParam param,
                                 GeoMesaTableSchema geomesaTableSchema) {
    this.param = param;
    this.geomesaTableSchema = geomesaTableSchema;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    // UPSERT mode
    ChangelogMode.Builder builder = ChangelogMode.newBuilder();
    for (RowKind kind : requestedMode.getContainedKinds()) {
      if (kind != RowKind.UPDATE_BEFORE) {
        builder.addContainedKind(kind);
      }
    }
    return builder.build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    GeoMesaSinkFunction<RowData> sinkFunction = new GeoMesaSinkFunction<>(
            param, geomesaTableSchema,
            new RowDataToSimpleFeatureConverter(geomesaTableSchema));
    return SinkFunctionProvider.of(sinkFunction);
  }

  @Override
  public DynamicTableSink copy() {
    return new GeoMesaDynamicTableSink(param, geomesaTableSchema);
  }

  @Override
  public String asSummaryString() {
    return "GeoMesa";
  }
}
