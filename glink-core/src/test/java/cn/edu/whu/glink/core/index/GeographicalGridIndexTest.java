package cn.edu.whu.glink.core.index;

import org.junit.Test;
import org.locationtech.jts.geom.*;

import java.util.List;

public class GeographicalGridIndexTest {

  private final GeographicalGridIndex gridIndex = new GeographicalGridIndex(2);
  private final GeographicalGridIndex gridIndex1 =
          new GeographicalGridIndex(100, 110, 20, 40, 4, 8);

  @Test
  public void getPointIndexTest() {
    long index = gridIndex.getIndex(100, 40);
    long[] xy = gridIndex.getXY(index);
    System.out.println("[" + xy[0] + ", " + xy[1] + "]");

    long index1 = gridIndex1.getIndex(105, 30);
    long[] xy1 = gridIndex1.getXY(index1);
    System.out.println("[" + xy1[0] + ", " + xy1[1] + "]");
  }

  @Test
  public void getEnvelopIndexTest() {
    Envelope envelope = new Envelope(103, 107, 23, 34);

    List<Long> indices = gridIndex.getIndex(envelope);
    for (Long index : indices) {
      long[] xy = gridIndex.getXY(index);
      System.out.println("[" + xy[0] + ", " + xy[1] + "]");
    }

    List<Long> indices1 = gridIndex1.getIndex(envelope);
    for (Long index : indices1) {
      long[] xy1 = gridIndex1.getXY(index);
      System.out.println("[" + xy1[0] + ", " + xy1[1] + "]");
    }
  }

  @Test
  public void getIntersectIndexTest() {
    GeometryFactory factory = new GeometryFactory();
    Coordinate[] cs = new Coordinate[4];
    cs[0] = new Coordinate(10, 20);
    cs[1] = new Coordinate(100, 25);
    cs[2] = new Coordinate(50, 30);
    cs[3] = cs[0];
    Geometry geometry = factory.createPolygon(cs);
    System.out.println(geometry.getEnvelopeInternal());
    List<Long> indexes = gridIndex.getIntersectIndex(geometry);
    for (long index : indexes) {
      System.out.println(index);
    }
  }

  @Test
  public void beijingDistrictTest() {
    Envelope env = new Envelope(115.41, 117.51, 39.44, 41.06);
    GeographicalGridIndex index1 = new GeographicalGridIndex(15);
    GeographicalGridIndex index2 = new GeographicalGridIndex(env, 5, 4);

    Envelope env1 = new Envelope(115.411, 117.509, 39.441, 41.059);
    List<Long> r1 = index1.getIndex(env1);
    List<Long> r2 = index2.getIndex(env1);

    System.out.println(r1.size());
    System.out.println(r2.size());
  }

}