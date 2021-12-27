package cn.edu.whu.glink.connector.geomesa.util;

import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Enumeration of types supported by geomesa.
 * For more details please refer to https://www.geomesa.org/documentation/stable/user/datastores/attributes.html
 *
 * @author Yu Liebing
 * */
public enum GeoMesaType {
  // basic types
  STRING("String"),
  INTEGER("Integer"),
  DOUBLE("Double"),
  LONG("Long"),
  FLOAT("Float"),
  BOOLEAN("Boolean"),
  UUID("UUID"),
  DATE("Date"),
  TIMESTAMP("Timestamp"),
  // Geometry types
  POINT("Point"),
  LINE_STRING("LineString"),
  POLYGON("Polygon"),
  MULTI_POINT("MultiPoint"),
  MULTI_LINE_STRING("MultiLineString"),
  MULTI_POLYGON("MultiPolygon"),
  GEOMETRY_COLLECTION("GeometryCollection"),
  GEOMETRY("Geometry"),
  BYTES("Bytes");
  // complex types

  private static final int MIN_TIMESTAMP_PRECISION = 0;
  private static final int MAX_TIMESTAMP_PRECISION = 3;
  private static final int MIN_TIME_PRECISION = 0;
  private static final int MAX_TIME_PRECISION = 3;

  private final String name;

  GeoMesaType(String name) {
    this.name = name;
  }

  public static GeoMesaType getGeomesaType(String name) {
    GeoMesaType[] types = GeoMesaType.values();
    for (GeoMesaType type : types) {
      if (type.name.equalsIgnoreCase(name))
        return type;
    }
    throw new IllegalArgumentException("[" + GeoMesaType.class + "] Unsupported geometry type:" + name);
  }

  public static GeoMesaType mapLogicalTypeToGeomesaType(LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case CHAR:          // CHAR / VARCHAR / STRING -> String
      case VARCHAR:
        return STRING;
      case BOOLEAN:       // BOOLEAN -> Boolean
        return BOOLEAN;
      case BINARY:        // BINARY / VARBINARY -> byte[]
      case VARBINARY:
        return BYTES;
      case TINYINT:       // TINYINT / SMALLINT / INT -> Integer
      case SMALLINT:
      case INTEGER:
        return INTEGER;
      case BIGINT:        // BIGINT -> Long
        return LONG;
      case FLOAT:         // FLOAT -> Float
        return FLOAT;
      case DOUBLE:        // DOUBLE -> Double
        return DOUBLE;
      case DATE:          // DATE / TIME -> Date
        return DATE;
      case TIME_WITHOUT_TIME_ZONE:
        final int timePrecision = getPrecision(logicalType);
        if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIME type is out of the range [%s, %s] supported by "
                          + "HBase connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
        return DATE;
      case TIMESTAMP_WITHOUT_TIME_ZONE: // TIMESTAMP -> Timestamp
        final int timestampPrecision = getPrecision(logicalType);
        if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                          + "HBase connector", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
        }
        return TIMESTAMP;
      default:  // unsupported: DECIMAL / TIMESTAMP WITH TIME ZONE / TIMESTAMP WITH LOCAL TIME ZONE
                // INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }
}
