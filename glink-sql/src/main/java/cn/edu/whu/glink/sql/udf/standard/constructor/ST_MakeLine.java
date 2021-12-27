package cn.edu.whu.glink.sql.udf.standard.constructor;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.*;
import org.xml.sax.SAXParseException;

import java.util.List;

@SuppressWarnings("checkstyle:TypeName")
public class ST_MakeLine extends ScalarFunction {

  private transient GeometryFactory geometryFactory;

  @Override
  public void open(FunctionContext context) throws Exception {
    geometryFactory = SpatialDataStream.geometryFactory;
  }

  public @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry eval(List<Point> points) {
    return geometryFactory
            .createLineString(points.stream()
                    .map(Point::getCoordinate)
                    .toArray(Coordinate[]::new));
  }
}
