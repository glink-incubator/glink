package cn.edu.whu.glink.core.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public abstract class GridIndex implements Serializable {

  protected static final int MAX_BITS = 30;

  // Bounds of the map. In geographical x means lng, y means lat.
  protected double minX;
  protected double maxX;
  protected double minY;
  protected double maxY;

  // Distance in each axis
  protected double xDis;
  protected double yDis;

  /**
   * Resolution of grids.
   * If the grid index is not defined by resolution, it should be -1;
   * */
  protected int res = -1;

  public int getRes() {
    return res;
  }

  public long getIndex(double x, double y) {
    long xx = (long) ((x - minX) / xDis);
    long yy = (long) ((y - minY) / yDis);
    return combineXY(xx, yy);
  }

  public long getIndex(Point p) {
    return getIndex(p.getX(), p.getY());
  }

  public List<Long> getIndex(Envelope envelope) {
    long minXX = (long) ((envelope.getMinX() - minX) / xDis);
    long maxXX = (long) ((envelope.getMaxX() - minX) / xDis);
    long minYY = (long) ((envelope.getMinY() - minY) / yDis);
    long maxYY = (long) ((envelope.getMaxY() - minY) / yDis);
    List<Long> indexes = new ArrayList<>((int) ((maxXX - minXX + 1) * (maxYY - minYY + 1)));
    for (long x = minXX; x <= maxXX; ++x) {
      for (long y = minYY; y <= maxYY; ++y) {
        indexes.add(combineXY(x, y));
      }
    }
    return indexes;
  }

  public List<Long> getIndex(Geometry geom) {
    if (geom instanceof Point) {
      Point p = (Point) geom;
      long index = getIndex(p.getX(), p.getY());
      return new ArrayList<Long>(1) {{ add(index); }};
    } else {
      return getIntersectIndex(geom);
    }
  }

  public abstract List<Long> getIndex(double x, double y, double distance);

  /**
   * Use for pair range join for analysis applications like DBSCAN.
   * */
  public abstract List<Long> getIndex(double x, double y, double distance, boolean reduce);

  /**
   * Get the indexes intersecting with the boundary of the input geometry.
   * @param geom The geometry to get a boundary.
   * @return Intersected indexes.
   */
  @SuppressWarnings("checkstyle:TodoComment")
  public List<Long> getIntersectIndex(Geometry geom) {
    // TODO: optimize this for irregular polygons
    return getIndex(geom.getEnvelopeInternal());
  }

  /**
   * Only for test.
   * */
  public long[] getXY(long index) {
    long y = index & 0x3fffffff;
    long x = index >> MAX_BITS;
    return new long[] {x, y};
  }

  protected long combineXY(long x, long y) {
    return x << MAX_BITS | y;
  }

  protected List<Long> getReduceIndex(double x, double y, Envelope envelope, double distance) {
    long minXX = (long) ((envelope.getMinX() - minX) / xDis);
    long maxXX = (long) ((envelope.getMaxX() - minX) / xDis);
    long minYY = (long) ((envelope.getMinY() - minY) / yDis);
    long maxYY = (long) ((envelope.getMaxY() - minY) / yDis);
    List<Long> res = new ArrayList<>(3);
    if (minXX == maxXX && minYY == maxYY) {
      return res;
    } else if (minXX == maxXX && minYY < maxYY) {
      res.add(combineXY(minXX, maxYY));
      return res;
    } else if (minXX < maxXX && minYY == maxYY) {
      res.add(combineXY(minXX, minYY));
      return res;
    } else {
      long curX = (long) ((x - minX) / xDis);
      long curY = (long) ((y - minY) / yDis);
      if (curX == minXX && curY == minYY) {
        res.add(combineXY(minXX, maxYY));
        res.add(combineXY(maxXX, maxYY));
        return res;
      } else if (curX == minXX && curY == maxYY) {
        return res;
      } else if (curX == maxXX && curY == maxYY) {
        res.add(combineXY(minXX, maxYY));
        return res;
      } else if (curX == maxXX && curY == minYY) {
        res.add(combineXY(minXX, minYY));
        res.add(combineXY(minXX, maxYY));
        res.add(combineXY(maxXX, maxYY));
        return res;
      }
    }
    return res;
  }
}
