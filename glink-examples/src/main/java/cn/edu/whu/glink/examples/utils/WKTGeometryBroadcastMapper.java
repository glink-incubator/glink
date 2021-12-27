package cn.edu.whu.glink.examples.utils;

import cn.edu.whu.glink.core.datastream.BroadcastSpatialDataStream;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parse text line to {@link Tuple2}(Boolean, Point) used by {@link BroadcastSpatialDataStream}.
 * <p>Text line format: id,wkt_string
 *
 * @author Yu Liebing
 * */
public class WKTGeometryBroadcastMapper implements FlatMapFunction<String, Tuple2<Boolean, Geometry>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WKTGeometryBroadcastMapper.class);

  @Override
  public void flatMap(String text, Collector<Tuple2<Boolean, Geometry>> collector) {
    try {
      String[] list = text.split(";");
      Geometry geometry = SpatialDataStream.wktReader.read(list[1]);
      Tuple1<String> t = new Tuple1<>(list[0]);
      geometry.setUserData(t);
      collector.collect(new Tuple2<>(true, geometry));
    } catch (Exception e) {
      LOGGER.error("Cannot parse line: {}", text);
    }
  }
}
