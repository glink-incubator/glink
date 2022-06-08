package cn.edu.whu.glink.core.geom;

import cn.edu.whu.glink.core.distance.DistanceCalculator;
import org.locationtech.jts.geom.*;

/**
 * @author Yu Liebing
 * */
public class Circle extends Geometry {

  private static final double FACTOR = 0.7071067811865476;

  private Geometry centerGeometry;
  private double radius;
  private Envelope env;
  private Envelope inscribedEnv;

  private final DistanceCalculator distanceCalculator;

  public Circle(Point center, double radius, DistanceCalculator distanceCalculator) {
    super(center.getFactory());
    this.centerGeometry = center;
    this.radius = radius;
    this.distanceCalculator = distanceCalculator;
  }

  public Circle(Geometry centerGeometry, double radius, DistanceCalculator distanceCalculator) {
    super(centerGeometry.getFactory());
    this.centerGeometry = centerGeometry;
    this.radius = radius;
    this.distanceCalculator = distanceCalculator;
  }

  public Geometry getCenter() {
    return centerGeometry;
  }

  public Envelope getInscribedEnv() {
    if (inscribedEnv == null) {
      inscribedEnv = distanceCalculator.calcBoxByDist(centerGeometry, radius * FACTOR);
    }
    return inscribedEnv;
  }

  @Override
  public String getGeometryType() {
    return "Circle";
  }

  @Override
  public Coordinate getCoordinate() {
    return centerGeometry.getCoordinate();
  }

  @Override
  public Coordinate[] getCoordinates() {
    return centerGeometry.getCoordinates();
  }

  @Override
  public int getNumPoints() {
    return 1;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public int getDimension() {
    return 0;
  }

  @Override
  public Geometry getBoundary() {
    return null;
  }

  @Override
  public int getBoundaryDimension() {
    return 0;
  }

  @Override
  public Geometry reverse() {
    return null;
  }

  @Override
  protected Geometry reverseInternal() {
    return null;
  }

  @Override
  public boolean equalsExact(Geometry geometry, double v) {
    return false;
  }

  @Override
  public void apply(CoordinateFilter coordinateFilter) {

  }

  @Override
  public void apply(CoordinateSequenceFilter coordinateSequenceFilter) {

  }

  @Override
  public void apply(GeometryFilter geometryFilter) {

  }

  @Override
  public void apply(GeometryComponentFilter geometryComponentFilter) {

  }

  @Override
  protected Geometry copyInternal() {
    return null;
  }

  @Override
  public void normalize() {

  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    if (env == null) {
      env = distanceCalculator.calcBoxByDist(centerGeometry, radius);
    }
    return env;
  }

  @Override
  protected int compareToSameClass(Object o) {
    return 0;
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator coordinateSequenceComparator) {
    return 0;
  }

  @Override
  protected int getTypeCode() {
    return 0;
  }

  @Override
  public Point getCentroid() {
    return centerGeometry.getCentroid();
  }

  @Override
  public boolean contains(Geometry g) {
    if (g instanceof Point) {
      if (!this.getEnvelopeInternal().contains(g.getEnvelopeInternal()))
        return false;
      if (this.getInscribedEnv().contains(g.getEnvelopeInternal()))
        return true;
      return distanceCalculator.calcDistance(centerGeometry, g) <= radius;
    }
    return super.contains(g);
  }
}
