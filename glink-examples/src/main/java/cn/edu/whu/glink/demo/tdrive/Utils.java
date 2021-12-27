package cn.edu.whu.glink.demo.tdrive;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 * */
public class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static List<Geometry> readBeijingDistricts(String path) {
    List<Geometry> polygons = new ArrayList<>(16);
    File file = new File(path);
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] items = line.split(";");
        Geometry polygon = SpatialDataStream.wktReader.read(items[2]);
        polygons.add(polygon);
      }
    } catch (IOException e) {
      LOGGER.error("Cannot read Beijing district file from {}", path, e);
    } catch (ParseException e) {
      LOGGER.error("Parse exception", e);
    }
    return polygons;
  }

  public static class BeijingDistrictFlatMapper implements
          FlatMapFunction<String, Tuple2<Boolean, Geometry>> {

    @Override
    public void flatMap(String line,
                        Collector<Tuple2<Boolean, Geometry>> collector)
            throws Exception {
      String[] items = line.split(";");
      Geometry geometry = SpatialDataStream.wktReader.read(items[2]);
      Tuple2<String, String> userData = new Tuple2<>(items[0], items[1]);
      geometry.setUserData(userData);
      collector.collect(new Tuple2<>(true, geometry));
    }
  }

  public static class TDriveFlatMapper extends RichFlatMapFunction<String, Point2> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TDriveFlatMapper.class);

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
        long timestamp = LocalDateTime.parse(
                items[1], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        double lng = Double.parseDouble(items[2]);
        double lat = Double.parseDouble(items[3]);
        Point2 p = new Point2(id, timestamp, lng, lat);
        collector.collect(p);
      } catch (Exception e) {
        LOGGER.error("Cannot parse line: {}", line);
      }
    }
  }

  public static class TDriveWithPidFlatMapper extends RichFlatMapFunction<String, Point2> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TDriveWithPidFlatMapper.class);

    private DateTimeFormatter formatter;

    @Override
    public void open(Configuration parameters) {
      formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void flatMap(String line, Collector<Point2> collector) {
      try {
        String[] items = line.split(",");
        String pid = items[0];
        String cid = items[1];
        long timestamp = LocalDateTime.parse(
                items[2], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        double lng = Double.parseDouble(items[3]);
        double lat = Double.parseDouble(items[4]);
        Point2 p = new Point2(pid, timestamp, lng, lat);
        p.setUserData(new Tuple1<>(cid));
        collector.collect(p);
      } catch (Exception e) {
        LOGGER.error("Cannot parse line: {}", line);
      }
    }
  }

  public static class SpatialDimensionJoinFunction implements
          JoinFunction<Point2, Geometry, String> {

    @Override
    public String join(Point2 p, Geometry g) throws Exception {
      StringBuilder sb = new StringBuilder();
      sb.append(p.getId()).append(",");
      sb.append(p.getTimestamp()).append(",");
      sb.append(p.getX()).append(",");
      sb.append(p.getY()).append(",");
      Tuple1<Long> pUserData = (Tuple1<Long>) p.getUserData();
      sb.append(pUserData.f0).append(",");
      if (g != null) {
        Tuple2<String, String> userData = (Tuple2<String, String>) g.getUserData();
        sb.append(userData.f0).append(",");
        sb.append(userData.f1).append(",");
      }
      sb.append(System.currentTimeMillis());
      return sb.toString();
    }
  }
}
