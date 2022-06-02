package cn.edu.whu.glink.core.tile;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class TileGrid {

  public static final int MAX_LEVEL = 18;

  private int level;

  public TileGrid(int level) {
    this.level = level;
  }

  public Tile getTile(Geometry geometry) {
    double[] pointXY = getPointCoordinate(geometry);
    return getTile(pointXY[0], pointXY[1]);
  }
  public Pixel getPixel(Geometry geometry) {
    double[] pointXY = getPointCoordinate(geometry);
    return getPixel(pointXY[0], pointXY[1]);
  }

  public Tile getTile(double lng, double lat) {
    double[] tileXY = getTileXYDouble(lng, lat);
    return new Tile(level, (int) tileXY[0], (int) tileXY[1]);
  }

  public Pixel getPixel(double lng, double lat) {
    double[] tileXY = getTileXYDouble(lng, lat);
    Tile tile = getTile(lng, lat);
    int pixelX = (int) (tileXY[0] * 256 % 256);
    int pixelY = (int) (tileXY[1] * 256 % 256);
    int pixelNo = pixelX + pixelY * 256;
    return new Pixel(tile, pixelX, pixelY, pixelNo);
  }

  public double[] getPointCoordinate(Geometry geometry) {
    double lat;
    double lng;
    if (geometry instanceof Point) {
      Point p = (Point) geometry;
      lng = p.getX();
      lat = p.getY();
    } else {
      Point centroid = geometry.getCentroid();
      lng = centroid.getX();
      lat = centroid.getY();
    }
    return new double[] {lng, lat};
  }

//   Use web Mercator projection.
//   The first item is the column number (x) of the tile.
//   The second item is the row number (y) of the tile.
  private double[] getTileXYDouble(double lng, double lat) {
    double n = Math.pow(2, level);
    double  tileX = (lng + 180) / 360 * n;
    double  tileY = ((1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n);
    return new double[] {tileX, tileY};
  }
}
