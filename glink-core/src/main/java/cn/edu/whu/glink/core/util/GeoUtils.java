package cn.edu.whu.glink.core.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.ShapeFactory;

/**
 * Geographical utils based on spatial4j.
 *
 * @author Yu Liebing
 * */
public class GeoUtils {

  public static final SpatialContext SPATIAL_CONTEXT = SpatialContext.GEO;
  public static final DistanceCalculator DISTANCE_CALCULATOR = SPATIAL_CONTEXT.getDistCalc();
  public static final ShapeFactory SHAPE_FACTORY = SPATIAL_CONTEXT.getShapeFactory();

  private static final double MIN_LNG = -180.;
  private static final double MAX_LNG = 180.;
  private static final double MIN_LAT = -90.;
  private static final double MAX_LAT = 90.;

  public static double distanceToDEG(double distance) {
    return distance * DistanceUtils.KM_TO_DEG;
  }

  /**
   * Calculate distance of two geometries. If the geometry is not point, use the centroid
   * of the geometry to calculate.
   * */
  public static double calcDistance(Geometry geom1, Geometry geom2) {
    org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
    org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
    return calcDistance(p1.getX(), p1.getY(), p2.getX(), p2.getY());
  }

  public static double calcDistance(double lng1, double lat1, double lng2, double lat2) {
    Point point1 = SHAPE_FACTORY.pointXY(checkLng(lng1), checkLat(lat1));
    Point point2 = SHAPE_FACTORY.pointXY(checkLng(lng2), checkLat(lat2));
    return SPATIAL_CONTEXT.calcDistance(point1, point2) * DistanceUtils.DEG_TO_KM;
  }

  /**
   * Calculate the envelop.
   *
   * @param p the center point
   * @param dis distance km
   * */
  public static Envelope calcEnvelopeByDis(org.locationtech.jts.geom.Point p, double dis) {
    return calcEnvelopeByDis(p.getX(), p.getY(), dis);
  }

  public static Envelope calcEnvelopeByDis(double lng, double lat, double dis) {
    Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
    Rectangle rect = DISTANCE_CALCULATOR.calcBoxByDistFromPt(
            point, dis * DistanceUtils.KM_TO_DEG, SPATIAL_CONTEXT, null);
    return new Envelope(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
  }

  public static Coordinate calcPointOnBearing(double lng, double lat, double angle, double dis) {
    Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
    Point result = DISTANCE_CALCULATOR.pointOnBearing(
            point, dis * DistanceUtils.KM_TO_DEG, angle, SPATIAL_CONTEXT, null);
    return new Coordinate(result.getX(), result.getY());
  }

  private static double checkLng(double lng) {
    if (lng < MIN_LNG) {
      return MIN_LNG;
    }
    return Math.min(lng, MAX_LNG);
  }

  private static double checkLat(double lat) {
    if (lat < MIN_LAT) {
      return MIN_LAT;
    }
    return Math.min(lat, MAX_LAT);
  }
}
