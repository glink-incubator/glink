package cn.edu.whu.glink.sql.udf.standard.output;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

@SuppressWarnings("checkstyle:TypeName")
public class ST_AsBinary extends ScalarFunction {

  private transient WKBWriter wkbWriter;

  @Override
  public void open(FunctionContext context) throws Exception {
    wkbWriter = SpatialDataStream.wkbWriter;
  }

  public byte[] eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry geom) {
    return wkbWriter.write(geom);
  }
}
