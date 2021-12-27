package cn.edu.whu.glink.examples.datagen;

import cn.edu.whu.glink.core.util.GeoUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

/**
 * Generate simple data for window dbscan test.
 *
 * @author Yu Liebing
 * */
public class GenDBSCANData {

  public static void main(String[] args) {
    double centerLng = 100, centerLat = 30;
    Envelope envelope = GeoUtils.calcEnvelopeByDis(centerLng, centerLat, 1);

    // cluster 1
    printCoordinate(1, new Coordinate(centerLng, centerLat));
    Coordinate c2 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 0, 0.1);
    printCoordinate(2, c2);
    Coordinate c3 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 90, 0.1);
    printCoordinate(3, c3);
    Coordinate c4 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 180, 0.1);
    printCoordinate(4, c4);
    Coordinate c5 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 270, 0.1);
    printCoordinate(5, c5);
    Coordinate c6 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 30, 0.1);
    printCoordinate(6, c6);

    Coordinate nextCenter = GeoUtils.calcPointOnBearing(centerLng, centerLat, 225, 0.5);
    centerLng = nextCenter.getX();
    centerLat = nextCenter.getY();

    // cluster 2
    printCoordinate(7, nextCenter);
    Coordinate c8 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 0, 0.1);
    printCoordinate(8, c8);
    Coordinate c9 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 30, 0.1);
    printCoordinate(9, c9);
    Coordinate c10 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 90, 0.1);
    printCoordinate(10, c10);
    Coordinate c11 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 180, 0.1);
    printCoordinate(11, c11);

    nextCenter = GeoUtils.calcPointOnBearing(centerLng, centerLat, 135, 0.5);
    centerLng = nextCenter.getX();
    centerLat = nextCenter.getY();

    // noise 1
    Coordinate c12 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 0, 0.1);
    printCoordinate(12, c12);
    Coordinate c13 = GeoUtils.calcPointOnBearing(centerLng, centerLat, 90, 0.1);
    printCoordinate(13, c13);
  }

  public static void printCoordinate(int id, Coordinate c) {
    System.out.println(id + "," + c.getX() + "," + c.getY() + "," + "2021-09-28 10:00:00");
  }
}
