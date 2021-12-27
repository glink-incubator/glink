package cn.edu.whu.glink.connector.geomesa.options;

import org.apache.flink.table.api.ValidationException;

/**
 * @author Yu Liebing
 * */
public class GeoMesaConfigOptionFactory {

  public static GeoMesaConfigOption createGeomesaConfigOption(String dataStore) {
    if (dataStore.equalsIgnoreCase("hbase")) {
      return new HBaseConfigOption();
    } else {
      throw new ValidationException(String.format("Unsupported data store: %s.", dataStore));
    }
  }
}
