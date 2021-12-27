package cn.edu.whu.glink.core.test.join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TDriveTrajectoryReader {

  private String path;

  public TDriveTrajectoryReader(String path) {
    this.path = path;
  }

  List<Point> read() throws FileNotFoundException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    GeometryFactory geometryFactory = new GeometryFactory();
    List<Point> points = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] list = line.split(",");
        int id = Integer.parseInt(list[0]);
        Date date = sdf.parse(list[1]);
        double lng = Double.parseDouble(list[2]);
        double lat = Double.parseDouble(list[3]);
        Point p = geometryFactory.createPoint(new Coordinate(lng, lat));
        Tuple2<Integer, Long> userData = new Tuple2<>(id, date.getTime());
        p.setUserData(userData);
        points.add(p);
      }
    } catch (IOException | ParseException e) {
      e.printStackTrace();
    }
    points.sort((p1, p2) -> {
      long t1 = ((Tuple2<Integer, Long>) p1.getUserData()).f1;
      long t2 = ((Tuple2<Integer, Long>) p2.getUserData()).f1;
      return (int) (t1 - t2);
    });
    return points;
  }
}
