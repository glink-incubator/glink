package cn.edu.whu.glink.core.geom;

import org.locationtech.jts.geom.*;

import java.util.Arrays;

/**
 * @author Yu Liebing
 * */
public final class MultiPolygonWithIndex extends MultiPolygon {

  private final PolygonWithIndex[] polygonWithIndices;

  private MultiPolygonWithIndex(Polygon[] polygons, GeometryFactory factory) {
    super(polygons, factory);
    polygonWithIndices = new PolygonWithIndex[polygons.length];
    for (int i = 0; i < polygons.length; ++i) {
      polygonWithIndices[i] = PolygonWithIndex.fromPolygon(polygons[i]);
    }
  }

  @Override
  public boolean contains(Geometry g) {
    if (g instanceof Point) {
      Point p = (Point) g;
      if (!this.getEnvelopeInternal().contains(p.getEnvelopeInternal()))
        return false;
      for (PolygonWithIndex polygonWithIndex : polygonWithIndices) {
        if (polygonWithIndex.contains(p)) return true;
      }
      return false;
    } else {
      return super.contains(g);
    }
  }

  public static MultiPolygonWithIndex fromMultiPolygon(MultiPolygon multiPolygon) {
    Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
    Arrays.setAll(polygons, multiPolygon::getGeometryN);
    MultiPolygonWithIndex multiPolygonWithIndex = new MultiPolygonWithIndex(polygons, multiPolygon.getFactory());
    multiPolygonWithIndex.setUserData(multiPolygon.getUserData());
    return multiPolygonWithIndex;
  }
}
