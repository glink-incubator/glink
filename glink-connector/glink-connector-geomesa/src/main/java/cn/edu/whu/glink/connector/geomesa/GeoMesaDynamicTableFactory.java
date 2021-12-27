package cn.edu.whu.glink.connector.geomesa;

import cn.edu.whu.glink.connector.geomesa.sink.GeoMesaDynamicTableSink;
import cn.edu.whu.glink.connector.geomesa.source.GeoMesaDynamicTableSource;
import cn.edu.whu.glink.connector.geomesa.options.GeoMesaConfigOption;
import cn.edu.whu.glink.connector.geomesa.options.GeoMesaConfigOptionFactory;
import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import cn.edu.whu.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import cn.edu.whu.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * GeoMesa connector factory.
 *
 * @author Yu Liebing
 * */
public class GeoMesaDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  private static final String IDENTIFIER = "geomesa";

  private GeoMesaConfigOption geomesaConfigOption;

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    TableSchema tableSchema = context.getCatalogTable().getSchema();
    Map<String, String> options = context.getCatalogTable().getOptions();
    validatePrimaryKey(tableSchema);

    String dataStore = options.get(GeoMesaConfigOption.GEOMESA_DATA_STORE.key());
    geomesaConfigOption = GeoMesaConfigOptionFactory.createGeomesaConfigOption(dataStore);

    TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    final ReadableConfig tableOptions = helper.getOptions();

    // get geomesa datastore params
    GeoMesaDataStoreParam geomesaDataStoreParam =
            GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam(dataStore);
    geomesaDataStoreParam.initFromConfigOptions(helper.getOptions());
    // convert flink table schema to geomesa schema
    GeoMesaTableSchema geomesaTableSchema =
            GeoMesaTableSchema.fromTableSchemaAndOptions(tableSchema, tableOptions);

    return new GeoMesaDynamicTableSink(geomesaDataStoreParam, geomesaTableSchema);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    TableSchema tableSchema = context.getCatalogTable().getSchema();
    Map<String, String> options = context.getCatalogTable().getOptions();
    validatePrimaryKey(tableSchema);

    String dataStore = options.get(GeoMesaConfigOption.GEOMESA_DATA_STORE.key());
    geomesaConfigOption = GeoMesaConfigOptionFactory.createGeomesaConfigOption(dataStore);

    TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    final ReadableConfig tableOptions = helper.getOptions();

    // get geomesa datastore params
    GeoMesaDataStoreParam geoMesaDataStoreParam =
            GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam(dataStore);
    geoMesaDataStoreParam.initFromConfigOptions(helper.getOptions());
    // convert flink table schema to geomesa schema
    GeoMesaTableSchema geoMesaTableSchema = GeoMesaTableSchema.fromTableSchemaAndOptions(
            tableSchema, tableOptions);

    return new GeoMesaDynamicTableSource(geoMesaDataStoreParam, geoMesaTableSchema);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>(geomesaConfigOption.getRequiredOptions());
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new HashSet<>(geomesaConfigOption.getOptionalOptions());
  }

  private static void validatePrimaryKey(TableSchema tableSchema) {
    if (!tableSchema.getPrimaryKey().isPresent()) {
      throw new IllegalArgumentException("GeoMesa table schema required to define a primary key.");
    }
    tableSchema.getPrimaryKey().ifPresent(k -> {
      if (1 != k.getColumns().size()) {
        throw new IllegalArgumentException("GeoMesa only supported one primary key.");
      }
    });
  }
}
