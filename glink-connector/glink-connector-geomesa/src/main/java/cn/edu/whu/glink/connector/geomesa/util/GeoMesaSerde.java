package cn.edu.whu.glink.connector.geomesa.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.feature.simple.SimpleFeature;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Helper class for serializing flink sql record to SimpleFeature.
 *
 * @author Yu Liebing
 * */
public class GeoMesaSerde {

  private static final int MIN_TIMESTAMP_PRECISION = 0;
  private static final int MAX_TIMESTAMP_PRECISION = 3;
  private static final int MIN_TIME_PRECISION = 0;
  private static final int MAX_TIME_PRECISION = 3;

  private static final WKTReader WKT_READER = new WKTReader();
  private static final WKTWriter WKT_WRITER = new WKTWriter();

  @FunctionalInterface
  public interface GeoMesaFieldEncoder extends Serializable {
    Object encode(RowData rowData, int pos);
  }

  public static GeoMesaFieldEncoder getGeoMesaFieldEncoder(LogicalType fieldType, boolean isSpatialField) {
    if (isSpatialField) {
      return ((rowData, pos) -> {
        try {
          return WKT_READER.read(rowData.getString(pos).toString());
        } catch (ParseException e) {
          throw new UnsupportedOperationException("Only support WKT string for spatial fields.");
        }
      });
    }
    // see GeomesaType for type mapping from flink sql to geomesa
    switch (fieldType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return ((rowData, pos) -> (rowData.getString(pos) == null ? null : rowData.getString(pos).toString()));
      case BOOLEAN:
        return (RowData::getBoolean);
      case BINARY:
      case VARBINARY:
        return (RowData::getBinary);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return RowData::getInt;
      case DATE:
        throw new UnsupportedOperationException("Currently not supported DATE, will be fix in the future");
      case TIME_WITHOUT_TIME_ZONE:
        final int timePrecision = getPrecision(fieldType);
        if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIME type is out of the range [%s, %s] supported by "
                          + "HBase connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
        throw new UnsupportedOperationException("Currently not supported TIME, will be fix in the future");
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return RowData::getLong;
      case FLOAT:
        return RowData::getFloat;
      case DOUBLE:
        return RowData::getDouble;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int timestampPrecision = getPrecision(fieldType);
        if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                          + "HBase connector", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
        }
        return (row, pos) -> {
          long timestamp = row.getTimestamp(pos, timestampPrecision).getMillisecond();
          return new Timestamp(timestamp);
        };
      default:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType);
    }
  }

  @FunctionalInterface
  public interface GeoMesaFieldDecoder extends Serializable {
    Object decode(SimpleFeature sf, int pos);
  }

  public static GeoMesaFieldDecoder getGeoMesaFieldDecoder(LogicalType fieldType, boolean isSpatialField) {
    if (isSpatialField) {
      return ((sf, pos) -> {
        String wkt = WKT_WRITER.write((Geometry) sf.getAttribute(pos));
        return StringData.fromString(wkt);
      });
    }
    switch (fieldType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return ((sf, pos) -> StringData.fromString((String) sf.getAttribute(pos)));
      case BOOLEAN:
      case BINARY:
      case VARBINARY:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case INTERVAL_DAY_TIME:
      case FLOAT:
      case DOUBLE:
        return SimpleFeature::getAttribute;
      case DATE:
        throw new UnsupportedOperationException("Currently not supported DATE, will be fix in the future");
      case TIME_WITHOUT_TIME_ZONE:
        final int timePrecision = getPrecision(fieldType);
        if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIME type is out of the range [%s, %s] supported by "
                          + "HBase connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
        throw new UnsupportedOperationException("Currently not supported TIME, will be fix in the future");
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int timestampPrecision = getPrecision(fieldType);
        if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                          + "HBase connector", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
        }
        return (sf, pos) -> {
          Date timestamp = (Date) sf.getAttribute(pos);
          return TimestampData.fromEpochMillis(timestamp.getTime());
        };
      default:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType);
    }
  }
}
