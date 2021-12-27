package cn.edu.whu.glink.examples.utils;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parse text line to {@link Point} with user data of Tuple1(id).
 * <p>Text line format: id,lng(x),lat(y)
 *
 * @author  Yu Liebing
 * */
public class SimplePointFlatMapper  extends RichFlatMapFunction<String, Point> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimplePointFlatMapper.class);

  private transient GeometryFactory factory;

  @Override
  public void open(Configuration parameters) {
    factory = SpatialDataStream.geometryFactory;
  }

  @Override
  public void flatMap(String line, Collector<Point> collector) {
    try {
      String[] items = line.split(",");
      int id = Integer.parseInt(items[0]);
      double lng = Double.parseDouble(items[1]);
      double lat = Double.parseDouble(items[2]);
      Point p = factory.createPoint(new Coordinate(lng, lat));
      p.setUserData(new Tuple1<>(id));
      collector.collect(p);
    } catch (Exception e) {
      LOGGER.error("Cannot parse line: {}", line);
    }
  }
}
