package cn.edu.whu.glink.core.distance;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class GeometricDistanceCalculator implements DistanceCalculator {

  @Override
  public double calcDistance(Geometry geom1, Geometry geom2) {
    Point p1 = geom1.getCentroid(), p2 = geom2.getCentroid();
    double x1 = p1.getX(), y1 = p1.getY();
    double x2 = p2.getX(), y2 = p2.getY();
    return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
  }

  @Override
  public Envelope calcBoxByDist(Geometry geom, double distance) {
    Point p;
    if (geom instanceof Point) {
      p = (Point) geom;
    } else {
      p = geom.getCentroid();
    }
    double x = p.getX(), y = p.getY();
    return new Envelope(x - distance, x + distance, y - distance, y + distance);
  }
}
