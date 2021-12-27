package cn.edu.whu.glink.sql.udf.standard.constructor;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

@SuppressWarnings("checkstyle:TypeName")
public class ST_MakePointM extends ScalarFunction {

  private transient GeometryFactory geometryFactory;

  @Override
  public void open(FunctionContext context) throws Exception {
    geometryFactory = SpatialDataStream.geometryFactory;
  }

  public @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry eval(
          double x, double y, double m) {
    return geometryFactory.createPoint(new Coordinate(x, y, m));
  }
}
