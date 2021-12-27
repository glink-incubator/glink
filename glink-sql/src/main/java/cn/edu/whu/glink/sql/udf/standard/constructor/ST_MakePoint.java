package cn.edu.whu.glink.sql.udf.standard.constructor;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

@SuppressWarnings("checkstyle:TypeName")
public class ST_MakePoint extends ScalarFunction {

  private transient GeometryFactory factory;

  @Override
  public void open(FunctionContext context) throws Exception {
    factory = SpatialDataStream.geometryFactory;
  }

  public @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry eval(double x, double y) {
    return factory.createPoint(new Coordinate(x, y));
  }
}
