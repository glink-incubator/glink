package cn.edu.whu.glink.connector.geomesa.options.param;

import cn.edu.whu.glink.connector.geomesa.options.HBaseConfigOption;
import org.apache.flink.configuration.ReadableConfig;

/**
 * HBase Data Store configuration parameters, for more details refer to:
 * <a href=https://www.geomesa.org/documentation/stable/user/hbase/usage.html>
 *   https://www.geomesa.org/documentation/stable/user/hbase/usage.html</a>
 *
 * @author Yu Liebing
 * */
public class HBaseDataStoreParam extends GeoMesaDataStoreParam {

  @Override
  public void initFromConfigOptions(ReadableConfig config) {
    super.initFromConfigOptions(config);
    config.getOptional(HBaseConfigOption.HBASE_CATALOG)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_CATALOG.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_ZOOKEEPERS)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_ZOOKEEPERS.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_URL)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_URL.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_CONFIG_PATHS)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_CONFIG_PATHS.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_CONFIG_XML)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_CONFIG_XML.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_CONNECTIONS_REUSE)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_CONNECTIONS_REUSE.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_REMOTE_FILTERING)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_REMOTE_FILTERING.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_SECURITY_ENABLED)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_SECURITY_ENABLED.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_THREADS)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_THREADS.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_RANGES_MAX_PER_EXTENDED_SCAN)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_RANGES_MAX_PER_EXTENDED_SCAN.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_RANGES_MAX_PER_COPROCESSOR_SCAN)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_RANGES_MAX_PER_COPROCESSOR_SCAN.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_ARROW_ENABLE)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_ARROW_ENABLE.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_BIN_ENABLE)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_BIN_ENABLE.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_DENSITY_ENABLE)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_DENSITY_ENABLE.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_STATS_ENABLE)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_STATS_ENABLE.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_YIELD_PARTIAL_RESULTS)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_YIELD_PARTIAL_RESULTS.key(), v));
    config.getOptional(HBaseConfigOption.HBASE_COPROCESSOR_SCAN_PARALLEL)
            .ifPresent(v -> params.put(HBaseConfigOption.HBASE_COPROCESSOR_SCAN_PARALLEL.key(), v));
  }
}
