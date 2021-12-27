package cn.edu.whu.glink.connector.geomesa.util;

import cn.edu.whu.glink.connector.geomesa.options.GeoMesaConfigOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.opengis.feature.simple.SimpleFeature;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Wang Haocheng
 */
public final class GeoMesaSQLTableSchema extends AbstractGeoMesaTableSchema {

  // join parameters, when do temporal table join with geomesa
  private static final TemporalJoinPredict DEFAULT_TEMPORAL_JOIN_PREDICT = TemporalJoinPredict.INTERSECTS;
  private double joinDistance = 0.d;
  private TemporalJoinPredict temporalJoinPredict = DEFAULT_TEMPORAL_JOIN_PREDICT;
  private List<GeoMesaSerde.GeoMesaFieldEncoder> fieldEncoders = new ArrayList<>();
  private List<GeoMesaSerde.GeoMesaFieldDecoder> fieldDecoders = new ArrayList<>();
  String[] fieldNames;
  DataType[] fieldTypes;
  String primaryKey;
  String[] names;

  public GeoMesaSQLTableSchema(TableSchema tableSchema, ReadableConfig readableConfig) {
    fieldNames = tableSchema.getFieldNames();
    fieldTypes = tableSchema.getFieldDataTypes();
    primaryKey = tableSchema.getPrimaryKey().get().getColumns().get(0);
    names = tableSchema.getFieldNames();
    this.readableConfig = readableConfig;
    init();
  }

  @Override
  public void init() {
    schemaName = readableConfig.get(GeoMesaConfigOption.GEOMESA_SCHEMA_NAME);
    indicesInfo = readableConfig.get(GeoMesaConfigOption.GEOMESA_INDICES_ENABLED);
    spatialFields = getSpatialFields();
    fieldsNameToType = new ArrayList<>();
    // get fieldsNameToType
    geometryFieldNameBeIndexed = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_GEOMETRY);
    dateBeIndexed = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_DATE);
    if (geometryFieldNameBeIndexed == null) {
      // 未明确指定Default geometry， 取第一个,如果没有spatialfields，保持为0。
      if (!spatialFields.isEmpty()) {
        geometryFieldNameBeIndexed = spatialFields.keySet().iterator().next();
      }
    }
    checkDefaultIndexFields(geometryFieldNameBeIndexed, dateBeIndexed, fieldNames);
    int primaryFiledIndex = getPrimaryFieldsIndexes()[0];
    for (int i = 0; i < fieldNames.length; ++i) {
      if (i == primaryFiledIndex) {
        GeoMesaType primaryKeyType = GeoMesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
        if (GeoMesaType.STRING != primaryKeyType) {
          throw new IllegalArgumentException("Geomesa only supports STRING primary key.");
        }
      }
      boolean isSpatialField = spatialFields.containsKey(fieldNames[i]);
      Tuple2<String, GeoMesaType> ft = new Tuple2<>();
      ft.f0 = fieldNames[i];
      if (isSpatialField) {
        ft.f1 = spatialFields.get(fieldNames[i]);
      } else {
        ft.f1 = GeoMesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
      }
      fieldsNameToType.add(ft);
      fieldEncoders.add(GeoMesaSerde.getGeoMesaFieldEncoder(fieldTypes[i].getLogicalType(), isSpatialField));
      fieldDecoders.add(GeoMesaSerde.getGeoMesaFieldDecoder(fieldTypes[i].getLogicalType(), isSpatialField));
      // temporal table join parameters
      String joinPredict = readableConfig.get(GeoMesaConfigOption.GEOMESA_TEMPORAL_JOIN_PREDICT);
      validTemporalJoinParam(joinPredict);
    }
  }

  @Override
  public int[] getPrimaryFieldsIndexes() {

    int ind = -1;
    for (String name : names) {
      ind++;
      if (name.equalsIgnoreCase(primaryKey)) {
        return new int[]{ind};
      }
    }
    return new int[]{-1};
  }



  @Override
  public Object getFieldValue(int offset, SimpleFeature sf) {
    return getFieldDecoder(offset).decode(sf, offset);
  }

  public GeoMesaSerde.GeoMesaFieldDecoder getFieldDecoder(int pos) {
    return fieldDecoders.get(pos);
  }

  public GeoMesaSerde.GeoMesaFieldEncoder getFieldEncoder(int pos) {
    return fieldEncoders.get(pos);
  }


  public TemporalJoinPredict getTemporalJoinPredict() {
    return temporalJoinPredict;
  }

  public double getJoinDistance() {
    return joinDistance;
  }

  private static void checkDefaultIndexFields(String defaultGeometry, String defaultDate, String[] fieldNames) {
    if (null == defaultGeometry && null == defaultDate) return;
    boolean isGeometryValid = false, isDateValid = false;
    for (String fieldName : fieldNames) {
      if (fieldName.equals(defaultGeometry)) isGeometryValid = true;
      if (fieldName.equals(defaultDate)) isDateValid = true;
    }
    if (null != defaultGeometry && !isGeometryValid)
      throw new IllegalArgumentException("The default geometry field is not in the table schema.");
    if (null != defaultDate && !isDateValid)
      throw new IllegalArgumentException("The default Date field is not in the table schema.");
  }

  private void validTemporalJoinParam(String joinPredict) {
    if (null == joinPredict) return;
    if (joinPredict.startsWith("R")) {
      String[] items = joinPredict.split(":");
      if (2 != items.length) {
        throw new IllegalArgumentException("geomesa.temporal.join.predict support R:<distance> format for distance join");
      }
      temporalJoinPredict = TemporalJoinPredict.RADIUS;
      joinDistance = Double.parseDouble(items[1]);
    } else {
      temporalJoinPredict = TemporalJoinPredict.getTemporalJoinPredict(joinPredict);
    }
  }
}
