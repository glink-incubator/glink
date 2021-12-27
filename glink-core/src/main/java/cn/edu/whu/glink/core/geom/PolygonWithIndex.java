package cn.edu.whu.glink.core.geom;

import cn.edu.whu.glink.core.index.STRTreeIndex;
import cn.edu.whu.glink.core.index.TreeIndex;
import org.locationtech.jts.geom.*;

import java.util.Arrays;
import java.util.List;

/**
 * @author Yu Liebing
 * */
public final class PolygonWithIndex extends Polygon {

  private double maxX = Long.MAX_VALUE;
  private final TreeIndex<LineString> edges = new STRTreeIndex<>();

  private PolygonWithIndex(LinearRing shell, LinearRing[] holes, GeometryFactory factory) {
    super(shell, holes, factory);
    Coordinate[] cs = this.getCoordinates();
    // create an R-Tree for edges
    for (int i = 0; i < cs.length; ++i) {
      maxX = Math.max(maxX, cs[i].getX());
      if (i == cs.length - 1) {
        edges.insert(factory.createLineString(new Coordinate[] {cs[i], cs[0]}));
      } else {
        edges.insert(factory.createLineString(new Coordinate[] {cs[i], cs[i + 1]}));
      }
    }
    maxX += 1;
  }

  @Override
  public boolean contains(Geometry g) {
    if (g instanceof Point) {
      Point p = (Point) g;
      if (!this.getEnvelopeInternal().contains(p.getEnvelopeInternal()))
        return false;
      return contains(p);
    } else {
      return super.contains(g);
    }
  }

  /**
   * Use the ray method to determine whether the point is within the polygon.
   * */
  private boolean contains(Point p) {
    double x = p.getX(), y = p.getY();
    Envelope envelope = new Envelope(x, maxX, y, y);
    List<LineString> potentialEdges = edges.query(envelope);
    boolean flag = false;
    for (LineString e : potentialEdges) {
      Coordinate c1 = e.getCoordinateN(0), c2 = e.getCoordinateN(1);
      double x1 = c1.getX(), y1 = c1.getY();
      double x2 = c2.getX(), y2 = c2.getY();
      // point and polygon coincide with fixed point
      if ((equal(x1, x) && equal(y1, y)) || (equal(x2, x) && equal(y2, y))) {
        return true;
      }
      if ((y > y1 && y <= y2) || (y > y2 && y <= y1)) {
        double crossX = (x1 - x2) * (y - y1) / (y1 - y2) + x1;
        // point on the edge
        if (equal(crossX, x)) {
          return true;
        }
        if (crossX > x) {
          flag = !flag;
        }
      }
    }
    return flag;
  }

  private boolean equal(double a, double b) {
    return Math.abs(a - b) < 1e-12;
  }

  public static PolygonWithIndex fromPolygon(Polygon polygon) {
    LinearRing shell = (LinearRing) polygon.getExteriorRing();
    LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
    Arrays.setAll(holes, polygon::getInteriorRingN);
    PolygonWithIndex polygonWithIndex = new PolygonWithIndex(shell, holes, polygon.getFactory());
    polygonWithIndex.setUserData(polygon.getUserData());
    return polygonWithIndex;
  }
}
