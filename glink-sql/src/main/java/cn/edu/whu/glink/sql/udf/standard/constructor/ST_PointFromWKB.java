package cn.edu.whu.glink.sql.udf.standard.constructor;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

@SuppressWarnings("checkstyle:TypeName")
public class ST_PointFromWKB extends ScalarFunction {

  private transient WKBReader wkbReader;

  @Override
  public void open(FunctionContext context) throws Exception {
    wkbReader = SpatialDataStream.wkbReader;
  }

  public @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry eval(byte[] wkb)
          throws ParseException {
    return wkbReader.read(wkb);
  }
}
