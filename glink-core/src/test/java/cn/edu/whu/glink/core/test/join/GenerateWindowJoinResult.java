package cn.edu.whu.glink.core.test.join;

import cn.edu.whu.glink.core.util.GeoUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Point;

import java.io.FileNotFoundException;
import java.util.List;

public class GenerateWindowJoinResult {

  public static void main(String[] args) throws FileNotFoundException {
    String path = args[0];
    int windowSeconds = Integer.parseInt(args[1]) * 1000;
    double distance = Double.parseDouble(args[2]);
    TDriveTrajectoryReader reader = new TDriveTrajectoryReader(path);
    List<Point> points = reader.read();

    int count = 0;
    for (Point p : points) {
      Tuple2<Integer, Long> t = (Tuple2<Integer, Long>) p.getUserData();
      long start = t.f1 - (t.f1 + windowSeconds) % windowSeconds;
      long end = start + windowSeconds;
      for (Point p2 : points) {
        Tuple2<Integer, Long> t2 = (Tuple2<Integer, Long>) p2.getUserData();
        if (t2.f1 >= start && t2.f1 < end) {
          if (GeoUtils.calcDistance(p, p2) <= distance) {
            System.out.println(p + ", " + t + "|" + p2 + ", " + t2 + ", " + GeoUtils.calcDistance(p, p2));
            ++count;
          }
        }
      }
    }
    System.out.println(count);
  }
}
