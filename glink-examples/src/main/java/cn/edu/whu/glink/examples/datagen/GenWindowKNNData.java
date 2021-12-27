package cn.edu.whu.glink.examples.datagen;

import cn.edu.whu.glink.core.util.GeoUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Generate simple data for window knn test.
 *
 * @author Yu Liebing
 * */
public class GenWindowKNNData {

  public static void main(String[] args) {
    double minX = 100, maxX = 101;
    double minY = 30, maxY = 31;

    int count = 100;
    List<Tuple2<Integer, Coordinate>> coordinates = new ArrayList<>(count);
    for (int i = 1; i <= count; ++i) {
      double x = randomDouble(minX, maxX);
      double y = randomDouble(minY, maxY);
      Coordinate c = new Coordinate(x, y);
      coordinates.add(new Tuple2<>(i, c));
      printCoordinate(i, c);
    }

    Coordinate queryC = new Coordinate(100.5, 30.5);
    int k = 3;
    PriorityQueue<Tuple2<Integer, Coordinate>> pq = new PriorityQueue<>(4, (t1, t2) -> {
      double d1 = GeoUtils.calcDistance(queryC.x, queryC.y, t1.f1.x, t1.f1.y);
      double d2 = GeoUtils.calcDistance(queryC.x, queryC.y, t2.f1.x, t2.f1.y);
      if (Math.abs(d1 - d2) < 1e-6) return 0;
      else return d2 - d1 > 0 ? 1 : -1;
    });
    for (Tuple2<Integer, Coordinate> t : coordinates) {
      pq.offer(t);
      if (pq.size() > k) pq.poll();
    }
    System.out.println(pq);
  }

  public static Random random = new Random();

  public static double randomDouble(double min, double max) {
    return random.nextDouble() * (max - min) + min;
  }

  public static void printCoordinate(int id, Coordinate c) {
    System.out.println(id + "," + c.getX() + "," + c.getY() + "," + "2021-11-15 10:00:00");
  }
}
