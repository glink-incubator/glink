package cn.edu.whu.glink.connector.geomesa.util;

import cn.edu.whu.glink.connector.geomesa.options.GeoMesaConfigOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.opengis.feature.simple.SimpleFeature;

import java.util.List;

/**
 * Helps to specify a Geomesa Table's schema.
 *
 * @author Yu Liebing
 * */
public final  class GeoMesaStreamTableSchema extends AbstractGeoMesaTableSchema {

    public GeoMesaStreamTableSchema(List<Tuple2<String, GeoMesaType>> fieldsNameToType, ReadableConfig readableConfig) {
        this.fieldsNameToType = fieldsNameToType;
        this.readableConfig = readableConfig;
        init();
    }

    public static GeoMesaStreamTableSchema createGeoMesaStreamTableSchema(List<Tuple2<String, GeoMesaType>> fieldNamesToTypes, ReadableConfig readableConfig) {
        return new GeoMesaStreamTableSchema(fieldNamesToTypes, readableConfig);
    }

    @Override
    public void init() {
        schemaName = readableConfig.get(GeoMesaConfigOption.GEOMESA_SCHEMA_NAME);
        geometryFieldNameBeIndexed = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_GEOMETRY);
        indicesInfo = readableConfig.get(GeoMesaConfigOption.GEOMESA_INDICES_ENABLED);
        dateBeIndexed = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_DATE);
        spatialFields = getSpatialFields();
        // 未明确指定Default geometry， 取第一个,如果没有spatialfields，保持为null。
        if (geometryFieldNameBeIndexed == null) {
            if (!spatialFields.isEmpty()) {
                geometryFieldNameBeIndexed = spatialFields.keySet().iterator().next();
            }
        }
    }

    @Override
    public int[] getPrimaryFieldsIndexes() {
        String primaryKeyNames = readableConfig.get(GeoMesaConfigOption.PRIMARY_FIELD_NAME);
        String[] namesArray = primaryKeyNames.split(",");
        int[] res = new int[namesArray.length];
        for (int i = 0; i < namesArray.length; i++) {
            res[i] = getFieldPos(namesArray[i]);
        }
        return res;
    }

    @Override
    public Object getFieldValue(int offset, SimpleFeature sf) {
        // fieldsNameToType中的offset转换到userdata中的offset.
        return sf.getAttribute(offset);
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

    private int convertOffsetInListToInUserData(int in) {
        int geoOffset = getIndexedGeometryFieldIndex();
        if (in == geoOffset) {
            return -1;
        } else if (in < geoOffset) {
            return in;
        } else {
            return in - 1;
        }
    }
}
