package cn.edu.whu.glink.connector.geomesa.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Set;

/**
 * GeoMesa config options, which is related to HBase.
 *
 * @author Yu Liebing
 * */
public class HBaseConfigOption extends GeoMesaConfigOption {

  // ------------------------------------------------------------------------
  // Used for HBase data store.
  // ------------------------------------------------------------------------

  /** Required */
  public static final ConfigOption<String> HBASE_CATALOG = ConfigOptions
          .key("hbase.catalog")
          .stringType()
          .noDefaultValue()
          .withDescription("The name of the GeoMesa catalog table");

  /** Optional */
  public static final ConfigOption<String> HBASE_ZOOKEEPERS = ConfigOptions
          .key("hbase.zookeepers")
          .stringType()
          .noDefaultValue()
          .withDescription("A comma-separated list of servers in the HBase zookeeper ensemble. "
                  + "This is optional, the preferred method for defining the HBase connection is with hbase-site.xml");

  /** Optional */
  public static final ConfigOption<String> HBASE_COPROCESSOR_URL = ConfigOptions
          .key("hbase.coprocessor.url")
          .stringType()
          .noDefaultValue()
          .withDescription("Path to the GeoMesa jar containing coprocessors, for auto registration");

  /** Optional */
  public static final ConfigOption<String> HBASE_CONFIG_PATHS = ConfigOptions
          .key("hbase.config.paths")
          .stringType()
          .noDefaultValue()
          .withDescription("Additional HBase configuration resource files (comma-delimited)");

  /** Optional */
  public static final ConfigOption<String> HBASE_CONFIG_XML = ConfigOptions
          .key("hbase.config.xml")
          .stringType()
          .noDefaultValue()
          .withDescription("Additional HBase configuration properties, as a standard XML <configuration> element");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_CONNECTIONS_REUSE = ConfigOptions
          .key("hbase.connections.reuse")
          .booleanType()
          .noDefaultValue()
          .withDescription("Re-use and share HBase connections, or create a new one for this data store");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_REMOTE_FILTERING = ConfigOptions
          .key("hbase.remote.filtering")
          .booleanType()
          .noDefaultValue()
          .withDescription("Can be used to disable remote filtering and coprocessors, "
                  + "for environments where custom code can’t be installed");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_SECURITY_ENABLED = ConfigOptions
          .key("hbase.security.enabled")
          .booleanType()
          .noDefaultValue()
          .withDescription("Enable HBase security (visibilities)");

  /** Optional */
  public static final ConfigOption<Integer> HBASE_COPROCESSOR_THREADS = ConfigOptions
          .key("hbase.coprocessor.threads")
          .intType()
          .noDefaultValue()
          .withDescription("The number of HBase RPC threads to use per coprocessor query");

  /** Optional */
  public static final ConfigOption<Integer> HBASE_RANGES_MAX_PER_EXTENDED_SCAN = ConfigOptions
          .key("hbase.ranges.max-per-extended-scan")
          .intType()
          .noDefaultValue()
          .withDescription("Max ranges per extended scan. Ranges will be grouped into scans based on this setting");

  /** Optional */
  public static final ConfigOption<Integer> HBASE_RANGES_MAX_PER_COPROCESSOR_SCAN = ConfigOptions
          .key("hbase.ranges.max-per-coprocessor-scan")
          .intType()
          .noDefaultValue()
          .withDescription("Max ranges per coprocessor scan. Ranges will be grouped into scans based on this setting");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_COPROCESSOR_ARROW_ENABLE = ConfigOptions
          .key("hbase.coprocessor.arrow.enable")
          .booleanType()
          .noDefaultValue()
          .withDescription("Disable coprocessor scans for Arrow queries, and use local encoding instead");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_COPROCESSOR_BIN_ENABLE = ConfigOptions
          .key("hbase.coprocessor.bin.enable")
          .booleanType()
          .noDefaultValue()
          .withDescription("Disable coprocessor scans for Bin queries, and use local encoding instead");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_COPROCESSOR_DENSITY_ENABLE = ConfigOptions
          .key("hbase.coprocessor.density.enable")
          .booleanType()
          .noDefaultValue()
          .withDescription("Disable coprocessor scans for density queries, and use local processing instead");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_COPROCESSOR_STATS_ENABLE = ConfigOptions
          .key("hbase.coprocessor.stats.enable")
          .booleanType()
          .noDefaultValue()
          .withDescription("Disable coprocessor scans for stat queries, and use local processing instead");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_COPROCESSOR_YIELD_PARTIAL_RESULTS = ConfigOptions
          .key("hbase.coprocessor.yield.partial.results")
          .booleanType()
          .noDefaultValue()
          .withDescription("Toggle coprocessors yielding partial results");

  /** Optional */
  public static final ConfigOption<Boolean> HBASE_COPROCESSOR_SCAN_PARALLEL = ConfigOptions
          .key("hbase.coprocessor.scan.parallel")
          .booleanType()
          .noDefaultValue().withDescription("Toggle extremely parallel coprocessor scans (bounded by RPC threads)");

  @Override
  public Set<ConfigOption<?>> getRequiredOptions() {
    Set<ConfigOption<?>> set = super.getRequiredOptions();
    set.add(HBASE_CATALOG);
    return set;
  }

  @Override
  public Set<ConfigOption<?>> getOptionalOptions() {
    Set<ConfigOption<?>> set = super.getOptionalOptions();
    // hbase data store
    set.add(HBASE_ZOOKEEPERS);
    set.add(HBASE_COPROCESSOR_URL);
    set.add(HBASE_CONFIG_PATHS);
    set.add(HBASE_CONFIG_XML);
    set.add(HBASE_CONNECTIONS_REUSE);
    set.add(HBASE_REMOTE_FILTERING);
    set.add(HBASE_SECURITY_ENABLED);
    set.add(HBASE_COPROCESSOR_THREADS);
    set.add(HBASE_RANGES_MAX_PER_EXTENDED_SCAN);
    set.add(HBASE_RANGES_MAX_PER_COPROCESSOR_SCAN);
    set.add(HBASE_COPROCESSOR_ARROW_ENABLE);
    set.add(HBASE_COPROCESSOR_BIN_ENABLE);
    set.add(HBASE_COPROCESSOR_DENSITY_ENABLE);
    set.add(HBASE_COPROCESSOR_STATS_ENABLE);
    set.add(HBASE_COPROCESSOR_YIELD_PARTIAL_RESULTS);
    set.add(HBASE_COPROCESSOR_SCAN_PARALLEL);
    return set;
  }
}
