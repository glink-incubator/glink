package cn.edu.whu.glink.examples.utils;

import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
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
public class SimpleSTPoint2FlatMapper extends RichFlatMapFunction<String, Point2> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSTPoint2FlatMapper.class);

  private DateTimeFormatter formatter;

  @Override
  public void open(Configuration parameters) {
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public void flatMap(String line, Collector<Point2> collector) {
    try {
      String[] items = line.split(",");
      String id = items[0];
      double lng = Double.parseDouble(items[1]);
      double lat = Double.parseDouble(items[2]);
      long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
      Point2 p = new Point2(id, timestamp, lng, lat);
      collector.collect(p);
    } catch (Exception e) {
      LOGGER.error("Cannot parse line: {}", line);
    }
  }
}
