package cn.edu.whu.glink.sql.udf.standard.relationship;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

@SuppressWarnings("checkstyle:TypeName")
public class ST_Intersects extends ScalarFunction {

  public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry geom1,
                      @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry geom2) {
    return geom1.intersects(geom2);
  }
}
