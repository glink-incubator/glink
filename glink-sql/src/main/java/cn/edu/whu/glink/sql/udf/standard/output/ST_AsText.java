package cn.edu.whu.glink.sql.udf.standard.output;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

@SuppressWarnings("checkstyle:TypeName")
public class ST_AsText extends ScalarFunction {

  public transient WKTWriter wktWriter;

  @Override
  public void open(FunctionContext context) throws Exception {
    wktWriter = SpatialDataStream.wktWriter;
  }

  public String eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry geom) {
    return wktWriter.write(geom);
  }
}
