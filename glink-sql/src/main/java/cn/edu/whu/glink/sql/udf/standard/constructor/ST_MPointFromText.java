package cn.edu.whu.glink.sql.udf.standard.constructor;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@SuppressWarnings("checkstyle:TypeName")
public class ST_MPointFromText extends ScalarFunction {

  private transient WKTReader wktReader;

  @Override
  public void open(FunctionContext context) throws Exception {
    wktReader = SpatialDataStream.wktReader;
  }

  public @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry eval(String wkt)
          throws ParseException {
    return wktReader.read(wkt);
  }
}
