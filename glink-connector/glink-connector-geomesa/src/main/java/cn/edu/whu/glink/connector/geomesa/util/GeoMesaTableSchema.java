package cn.edu.whu.glink.connector.geomesa.util;

import cn.edu.whu.glink.connector.geomesa.options.GeoMesaConfigOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.locationtech.geomesa.utils.geotools.SchemaBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helps to specify a GeoMesa table's schema.
 *
 * @author Yu Liebing
 * */
public final class GeoMesaTableSchema implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final TemporalJoinPredict DEFAULT_TEMPORAL_JOIN_PREDICT = TemporalJoinPredict.INTERSECTS;

  private String schemaName;
  private int primaryKeyIndex;
  private String defaultGeometry;
  private String defaultDate;
  // join parameters, when do temporal table join with geomesa
  private double joinDistance = 0.d;
  private TemporalJoinPredict temporalJoinPredict = DEFAULT_TEMPORAL_JOIN_PREDICT;
  private final List<Tuple2<String, GeoMesaType>> fieldNameToType = new ArrayList<>();
  private final List<GeoMesaSerde.GeoMesaFieldEncoder> fieldEncoders = new ArrayList<>();
  private final List<GeoMesaSerde.GeoMesaFieldDecoder> fieldDecoders = new ArrayList<>();

  private final Map<String, Serializable> indexedDateAttribute = new HashMap<>();

  private GeoMesaTableSchema() { }

  public int getFieldNum() {
    return fieldNameToType.size();
  }

  public SimpleFeatureType getSchema() {
    SchemaBuilder builder  = SchemaBuilder.builder();
    for (Tuple2<String, GeoMesaType> ft : fieldNameToType) {
      boolean isDefault = ft.f0.equals(defaultGeometry) || ft.f0.equals(defaultDate);
      switch (ft.f1) {
        case POINT:
          builder.addPoint(ft.f0, isDefault);
          break;
        case DATE:
        case TIMESTAMP:
          builder.addDate(ft.f0, isDefault);
          break;
        case LONG:
          builder.addLong(ft.f0);
          break;
        case UUID:
          builder.addUuid(ft.f0);
          break;
        case FLOAT:
          builder.addFloat(ft.f0);
          break;
        case DOUBLE:
          builder.addDouble(ft.f0);
          break;
        case STRING:
          builder.addString(ft.f0);
          break;
        case BOOLEAN:
          builder.addBoolean(ft.f0);
          break;
        case INTEGER:
          builder.addInt(ft.f0);
          break;
        case POLYGON:
          builder.addPolygon(ft.f0, isDefault);
          break;
        case GEOMETRY:
          break;
        case LINE_STRING:
          builder.addLineString(ft.f0, isDefault);
          break;
        case MULTI_POINT:
          builder.addMultiPoint(ft.f0, isDefault);
          break;
        case MULTI_POLYGON:
          builder.addMultiPolygon(ft.f0, isDefault);
          break;
        case MULTI_LINE_STRING:
          builder.addMultiLineString(ft.f0, isDefault);
          break;
        case GEOMETRY_COLLECTION:
          builder.addGeometryCollection(ft.f0, isDefault);
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + ft.f1);
      }
    }
    SimpleFeatureType sft = builder.build(schemaName);
    // add indexed dated attribute
    for (Map.Entry<String, Serializable> e : indexedDateAttribute.entrySet()) {
      sft.getUserData().put(e.getKey(), e.getValue());
    }
    return sft;
  }

  public GeoMesaSerde.GeoMesaFieldEncoder getFieldEncoder(int pos) {
    return fieldEncoders.get(pos);
  }

  public GeoMesaSerde.GeoMesaFieldDecoder getFieldDecoder(int pos) {
    return fieldDecoders.get(pos);
  }

  public String getPrimaryKey(RowData record) {
    return (String) fieldEncoders.get(primaryKeyIndex).encode(record, primaryKeyIndex);
  }

  public String getFieldName(int pos) {
    return fieldNameToType.get(pos).f0;
  }

  public TemporalJoinPredict getTemporalJoinPredict() {
    return temporalJoinPredict;
  }

  public double getJoinDistance() {
    return joinDistance;
  }

  public static GeoMesaTableSchema fromTableSchemaAndOptions(TableSchema tableSchema, ReadableConfig readableConfig) {
    GeoMesaTableSchema geomesaTableSchema = new GeoMesaTableSchema();
    // schema name
    geomesaTableSchema.schemaName = readableConfig.get(GeoMesaConfigOption.GEOMESA_SCHEMA_NAME);
    // primary key name
    String primaryKey = tableSchema.getPrimaryKey().get().getColumns().get(0);
    // spatial fields
    Map<String, GeoMesaType> spatialFields = getSpatialFields(
            readableConfig.get(GeoMesaConfigOption.GEOMESA_SPATIAL_FIELDS));
    // all fields and field encoders
    String[] fieldNames = tableSchema.getFieldNames();
    DataType[] fieldTypes = tableSchema.getFieldDataTypes();
    // default geometry and Data field
    geomesaTableSchema.defaultGeometry = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_GEOMETRY);
    geomesaTableSchema.defaultDate = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_DATE);
    checkDefaultIndexFields(geomesaTableSchema.defaultGeometry, geomesaTableSchema.defaultDate, fieldNames);
    for (int i = 0; i < fieldNames.length; ++i) {
      // check primary key
      if (primaryKey.equals(fieldNames[i])) {
        GeoMesaType primaryKeyType = GeoMesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
        if (primaryKeyType != GeoMesaType.STRING) {
          throw new IllegalArgumentException("Geomesa only supports STRING primary key.");
        }
        geomesaTableSchema.primaryKeyIndex = i;
      }
      boolean isSpatialField = spatialFields.containsKey(fieldNames[i]);
      Tuple2<String, GeoMesaType> ft = new Tuple2<>();
      ft.f0 = fieldNames[i];
      if (isSpatialField) {
        ft.f1 = spatialFields.get(fieldNames[i]);
      } else {
        ft.f1 = GeoMesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
      }
      geomesaTableSchema.fieldNameToType.add(ft);
      geomesaTableSchema.fieldEncoders.add(
              GeoMesaSerde.getGeoMesaFieldEncoder(fieldTypes[i].getLogicalType(), isSpatialField));
      geomesaTableSchema.fieldDecoders.add(
              GeoMesaSerde.getGeoMesaFieldDecoder(fieldTypes[i].getLogicalType(), isSpatialField));
    }
    // temporal table join parameters
    String joinPredict = readableConfig.get(GeoMesaConfigOption.GEOMESA_TEMPORAL_JOIN_PREDICT);
    geomesaTableSchema.validTemporalJoinParam(joinPredict);
    return geomesaTableSchema;
  }

  private static Map<String, GeoMesaType> getSpatialFields(String spatialFields) {
    Map<String, GeoMesaType> nameToType = new HashMap<>();
    if (spatialFields == null) return nameToType;
    String[] items = spatialFields.split(",");
    for (String item : items) {
      String[] nt = item.split(":");
      String name = nt[0];
      GeoMesaType type = GeoMesaType.getGeomesaType(nt[1]);
      nameToType.put(name, type);
    }
    return nameToType;
  }

  private static void checkDefaultIndexFields(String defaultGeometry, String defaultDate, String[] fieldNames) {
    if (defaultGeometry == null && defaultDate == null) return;
    boolean isGeometryValid = false, isDateValid = false;
    for (String fieldName : fieldNames) {
      if (fieldName.equals(defaultGeometry)) isGeometryValid = true;
      if (fieldName.equals(defaultDate)) isDateValid = true;
    }
    if (defaultGeometry != null && !isGeometryValid)
      throw new IllegalArgumentException("The default geometry field is not in the table schema.");
    if (defaultDate != null && !isDateValid)
      throw new IllegalArgumentException("The default Date field is not in the table schema.");
  }

  private void validTemporalJoinParam(String joinPredict) {
    if (joinPredict == null) return;
    if (joinPredict.startsWith("R")) {
      String[] items = joinPredict.split(":");
      if (items.length != 2) {
        throw new IllegalArgumentException("geomesa.temporal.join.predict support R:<distance> format for distance join");
      }
      temporalJoinPredict = TemporalJoinPredict.RADIUS;
      joinDistance = Double.parseDouble(items[1]);
    } else {
      temporalJoinPredict = TemporalJoinPredict.getTemporalJoinPredict(joinPredict);
    }
  }
}