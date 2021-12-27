package cn.edu.whu.glink.connector.geomesa.util;

import cn.edu.whu.glink.connector.geomesa.options.GeoMesaConfigOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.locationtech.geomesa.utils.geotools.SchemaBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Wang Haocheng
 * @date 2021/5/8 - 2:07 下午
 */
public abstract class AbstractGeoMesaTableSchema implements Serializable {
  String schemaName;
  String geometryFieldNameBeIndexed;
  String dateBeIndexed;
  String indicesInfo;
  Map<String, GeoMesaType> spatialFields;
  List<Tuple2<String, GeoMesaType>> fieldsNameToType;
  ReadableConfig readableConfig;


  protected AbstractGeoMesaTableSchema() {

  }

  /**
   * Get {@link SimpleFeatureType} to generate Simple features with specific schema.
   *
   * @return Simple feature type with default
   */
  public SimpleFeatureType getSimpleFeatureType() {
    SchemaBuilder builder = SchemaBuilder.builder();
    for (Tuple2<String, GeoMesaType> ft : fieldsNameToType) {
      boolean isDefault = ft.f0.equals(geometryFieldNameBeIndexed) || ft.f0.equals(dateBeIndexed);
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
        case BYTES:
          builder.addBytes(ft.f0);
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
    return builder.build(schemaName);
  }

  /**
   * Get the number of fields, including geometry fields, date fields and other attribute fields.
   * @return The number of fields
   */
  public int getFieldNum() {
    return fieldsNameToType.size();
  }

  /**
   * Get a map of spatial fields from {@link ReadableConfig} of {@link GeoMesaConfigOption}
   */
  Map<String, GeoMesaType> getSpatialFields() {
    String spatialFieldsInfo = readableConfig.get(GeoMesaConfigOption.GEOMESA_SPATIAL_FIELDS);
    Map<String, GeoMesaType> nameToType = new HashMap<>();
    if (null == spatialFieldsInfo) {
      return nameToType;
    }
    String[] items = spatialFieldsInfo.split(",");
    for (String item : items) {
      String[] nt = item.split(":");
      String name = nt[0];
      GeoMesaType type = GeoMesaType.getGeomesaType(nt[1]);
      nameToType.put(name, type);
    }
    return nameToType;
  }

  /**
   * When generate simple feature type, get the user-defined indices configuration and add to the simple feature type.
   */
  public String getIndicesInfo() {
    return indicesInfo;
  }

  private String getIndexedGeometryFieldName() {
    return geometryFieldNameBeIndexed;
  }

  public int getIndexedGeometryFieldIndex() {
    int offset = -1;
    String indGeometryName = getIndexedGeometryFieldName();
    for (Tuple2 tup : fieldsNameToType) {
      offset++;
      if (tup.f0.equals(indGeometryName)) {
        break;
      }
    }
    return offset == fieldsNameToType.size() ? -1 : offset;
  }

  /**
   * Get the field name of a position;
   */
  public String getFieldName(int pos) {
    return fieldsNameToType.get(pos).f0;
  }

  /**
   * Get the field index of a field name in the name to type list, if no such field name, return -1.
   */
  int getFieldPos(String name) {
    Iterator<Tuple2<String, GeoMesaType>> iterator = fieldsNameToType.iterator();
    int index = -1;
    while (iterator.hasNext()) {
      index++;
      if (iterator.next().f0.equalsIgnoreCase(name)) {
        return index;
      }
    }
    return -1;
  }

  /**
   * Get a specific field from SimpleFeature.
   * <p>Used in source.</p>
   * @param offset The offset from the name to type list.
   */
  public Object getFieldValue(int offset, SimpleFeature sf) {
    return sf.getAttribute(offset);
  };

  /**
   * Get the column indexes of the fields grouping a primary key, with the order it defined.
   * <p>Used in sink.</p>
   *
   * @return primary key name;
   */
  public abstract int[] getPrimaryFieldsIndexes();

  /**
   * Sets schema name, default geometry and date, indices, spatial fields.
   */
  public abstract void init();
}
