package cn.edu.whu.glink.connector.geomesa.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashSet;
import java.util.Set;

/**
 * GeoMesa config options, which is not related to backend storage.
 *
 * @author Yu Liebing
 */
public class GeoMesaConfigOption {

  // ------------------------------------------------------------------------
  // Used for glink geomesa connector.
  // ------------------------------------------------------------------------

  /**
   * Required
   */
  public static final ConfigOption<String> GEOMESA_DATA_STORE = ConfigOptions
          .key("geomesa.data.store")
          .stringType()
          .noDefaultValue()
          .withDescription("The geomesa data store backend");

  /**
   * Required
   */
  public static final ConfigOption<String> GEOMESA_SCHEMA_NAME = ConfigOptions
          .key("geomesa.schema.name")
          .stringType()
          .noDefaultValue()
          .withDescription("The geomesa schema name");

  // ------------------------------------------------------------------------
  // Used for geomesa data store.
  // ------------------------------------------------------------------------

  /**
   * Optional
   */
  public static final ConfigOption<String> GEOMESA_SECURITY_AUTHS = ConfigOptions
          .key("geomesa.security.auths")
          .stringType()
          .noDefaultValue()
          .withDescription("Comma-delimited superset of authorizations that will be used for queries");

  /**
   * Optional
   */
  public static final ConfigOption<Boolean> GEOMESA_SECURITY_FORCE_EMPTY_AUTHS = ConfigOptions
          .key("geomesa.security.force-empty-auths")
          .booleanType()
          .noDefaultValue()
          .withDeprecatedKeys("Forces authorizations to be empty");

  /**
   * Optional
   */
  public static final ConfigOption<Boolean> GEOMESA_QUERY_AUDIT = ConfigOptions
          .key("geomesa.query.audit")
          .booleanType()
          .noDefaultValue()
          .withDescription("Audit queries being run. Queries will be written to a log file");

  /**
   * Optional
   */
  public static final ConfigOption<String> GEOMESA_QUERY_TIMEOUT = ConfigOptions
          .key("geomesa.query.timeout")
          .stringType()
          .noDefaultValue()
          .withDescription("The max time a query will be allowed to run before being killed. "
                  + "The timeout is specified as a duration, e.g. 1 minute or 60 seconds");

  /**
   * Optional
   */
  public static final ConfigOption<Integer> GEOMESA_QUERY_THREADS = ConfigOptions
          .key("geomesa.query.threads")
          .intType()
          .noDefaultValue()
          .withDescription("The number of threads to use per query");

  /**
   * Optional
   */
  public static final ConfigOption<Boolean> GEOMESA_QUERY_LOOSE_BOUNDING_BOX = ConfigOptions
          .key("geomesa.query.loose-bounding-box")
          .booleanType()
          .noDefaultValue()
          .withDescription("Use loose bounding boxes - queries will be faster but may return extraneous results");

  /**
   * Optional
   */
  public static final ConfigOption<Boolean> GEOMESA_STATS_GENERATE = ConfigOptions
          .key("geomesa.stats.generate")
          .booleanType()
          .noDefaultValue()
          .withDescription("Toggle collection of statistics (currently not implemented)");

  /**
   * Optional
   */
  public static final ConfigOption<Boolean> GEOMESA_QUERY_CACHING = ConfigOptions
          .key("geomesa.query.caching")
          .booleanType()
          .noDefaultValue()
          .withDescription("Toggle caching of results");

  // ------------------------------------------------------------------------
  // Used to assist table schema create.
  // ------------------------------------------------------------------------

  /**
   * Optional. Required if exists spatial fields.
   */
  public static final ConfigOption<String> GEOMESA_SPATIAL_FIELDS = ConfigOptions
          .key("geomesa.spatial.fields")
          .stringType()
          .noDefaultValue()
          .withDescription("The spatial fields. Required if exists spatial fields");

  /**
   * Optional. If not set, the first declared geometry type will be indexed.
   */
  public static final ConfigOption<String> GEOMESA_DEFAULT_GEOMETRY = ConfigOptions
          .key("geomesa.default.geometry")
          .stringType()
          .noDefaultValue()
          .withDescription("The default geometry field to create spatial index");

  /**
   * Optional. If not set, the first declared Date type will be indexed.
   */
  public static final ConfigOption<String> GEOMESA_DEFAULT_DATE = ConfigOptions
          .key("geomesa.default.date")
          .stringType()
          .noDefaultValue()
          .withDescription("The default time field to create temporal index");

  public static final ConfigOption<String> PRIMARY_FIELD_NAME = ConfigOptions
          .key("geomesa.primary.field.name")
          .stringType()
          .noDefaultValue()
          .withDescription("The field to be treated as primary field.");

  // ------------------------------------------------------------------------
  // Used to index configuration.
  // ------------------------------------------------------------------------

  /**
   * Optional. Custom index configuration.
   */
  public static final ConfigOption<String> GEOMESA_INDICES_ENABLED = ConfigOptions
          .key("geomesa.indices.enabled")
          .stringType()
          .noDefaultValue()
          .withDescription("Custom index configuration.");

  /**
   * Used to instruct temporal table join spatial prediction
   */
  public static final ConfigOption<String> GEOMESA_TEMPORAL_JOIN_PREDICT = ConfigOptions
          .key("geomesa.temporal.join.predict")
          .stringType()
          .noDefaultValue()
          .withDescription("Used to instruct temporal table join spatial prediction, default is intersect");

  public Set<ConfigOption<?>> getRequiredOptions() {
    Set<ConfigOption<?>> set = new HashSet<>();
    // glink geomesa connector
    set.add(GEOMESA_DATA_STORE);
    set.add(GEOMESA_SCHEMA_NAME);
    return set;
  }

  public Set<ConfigOption<?>> getOptionalOptions() {
    Set<ConfigOption<?>> set = new HashSet<>();
    // geomesa data store optional options
    set.add(GEOMESA_SECURITY_AUTHS);
    set.add(GEOMESA_SECURITY_FORCE_EMPTY_AUTHS);
    set.add(GEOMESA_QUERY_AUDIT);
    set.add(GEOMESA_QUERY_TIMEOUT);
    set.add(GEOMESA_QUERY_THREADS);
    set.add(GEOMESA_QUERY_LOOSE_BOUNDING_BOX);
    set.add(GEOMESA_STATS_GENERATE);
    set.add(GEOMESA_QUERY_CACHING);
    // table schema
    set.add(GEOMESA_SPATIAL_FIELDS);
    set.add(GEOMESA_DEFAULT_GEOMETRY);
    set.add(GEOMESA_DEFAULT_DATE);
    set.add(GEOMESA_INDICES_ENABLED);
    // temporal table join
    set.add(GEOMESA_TEMPORAL_JOIN_PREDICT);
    return set;
  }
}
