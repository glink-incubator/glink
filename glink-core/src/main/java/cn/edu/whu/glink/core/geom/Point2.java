package cn.edu.whu.glink.core.geom;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.Objects;

/**
 * An extension class of {@link Point} with must contain id.
 *
 * @author Yu Liebing
 * */
public class Point2 extends Point {

  private final String id;
  private final long timestamp;

  public Point2(String id, double x, double y) {
    this(id, -1, x, y);
  }

  public Point2(String id, long timestamp, double x, double y) {
    super(SpatialDataStream.geometryFactory.createPoint(new Coordinate(x, y)).getCoordinateSequence(),
            SpatialDataStream.geometryFactory);
    this.id = id;
    this.timestamp = timestamp;
  }

  public Point2(String id, long timestamp, CoordinateSequence coordinates, GeometryFactory factory) {
    super(coordinates, factory);
    this.id = id;
    this.timestamp = timestamp;
  }

  public Point2(String id, long timestamp, Point point) {
    super(point.getCoordinateSequence(), point.getFactory());
    this.id = id;
    this.timestamp = timestamp;
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Point2 point2 = (Point2) o;
    return timestamp == point2.timestamp && Objects.equals(id, point2.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, timestamp);
  }
}
