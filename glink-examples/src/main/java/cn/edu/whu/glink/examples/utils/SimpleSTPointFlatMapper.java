package cn.edu.whu.glink.examples.utils;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Parse text line to {@link Point} with user data of Tuple2(id, timestamp).
 * <p>Text line format: id,lng(x),lat(y),yyyy-MM-dd HH:mm:ss
 *
 * @author  Yu Liebing
 * */
public class SimpleSTPointFlatMapper extends RichFlatMapFunction<String, Point> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSTPointFlatMapper.class);

  private GeometryFactory factory;
  private DateTimeFormatter formatter;

  @Override
  public void open(Configuration parameters) {
    factory = SpatialDataStream.geometryFactory;
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public void flatMap(String line, Collector<Point> collector) {
    try {
      String[] items = line.split(",");
      String id = items[0];
      double lng = Double.parseDouble(items[1]);
      double lat = Double.parseDouble(items[2]);
      long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
      Point p = factory.createPoint(new Coordinate(lng, lat));
      p.setUserData(new Tuple2<>(id, timestamp));
      collector.collect(p);
    } catch (Exception e) {
      LOGGER.error("Cannot parse line: {}", line);
    }
  }
}
