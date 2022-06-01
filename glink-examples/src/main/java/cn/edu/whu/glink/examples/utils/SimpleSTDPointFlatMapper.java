package cn.edu.whu.glink.examples.utils;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.locationtech.jts.geom.Point;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author Xu Qi
 */
public class SimpleSTDPointFlatMapper extends RichFlatMapFunction<String, Point> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSTDPointFlatMapper.class);
  private GeometryFactory factory;
  private DateTimeFormatter formatter;


  @Override
  public void open(Configuration parameters) throws Exception {
    factory = SpatialDataStream.geometryFactory;
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  }


  @Override
  public void flatMap(String line, Collector<Point> collector) throws Exception {
    try {
      String[] items = line.split(",");
      String id = items[0];
      double lng = Double.parseDouble(items[1]);
      double lat = Double.parseDouble(items[2]);
      long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
      Point p = factory.createPoint(new Coordinate(lng, lat));
      Tuple attributes = Tuple.newInstance(items.length - 2);
      for (int i = 0, j = 0; i < items.length; i++) {
        if (i == 0 || i > 2) {
          if (i == 3) {
            attributes.setField(timestamp, j);
          } else {
            attributes.setField(items[i], j);
          }
          j++;
        }
      }
      p.setUserData(attributes);
      collector.collect(p);
    } catch (Exception e) {
      LOGGER.error("Cannot parse line: {}", line);
    }
  }
}
