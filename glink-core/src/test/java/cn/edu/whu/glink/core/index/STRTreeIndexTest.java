package cn.edu.whu.glink.core.index;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Yu Liebing
 * */
public class STRTreeIndexTest {

  private final GeometryFactory factory = new GeometryFactory();
  private final WKTReader reader = new WKTReader();
  private final List<Point> points = new ArrayList<>();
  private final List<Polygon> polygons = new ArrayList<>();

  @Before
  public void init() throws ParseException {
    Point p1 = factory.createPoint(new Coordinate(114.0000001, 35.000000000000000000001));
    p1.setUserData(new Tuple2<>(1, "A"));
    Point p2 = factory.createPoint(new Coordinate(115, 34));
    p2.setUserData(new Tuple2<>(2, "B"));
    Point p3 = factory.createPoint(new Coordinate(100, 20));
    p3.setUserData(new Tuple2<>(3, "C"));
    Point p4 = factory.createPoint(new Coordinate(90, 10));
    p4.setUserData(new Tuple2<>(4, "D"));
    Point p5 = factory.createPoint(new Coordinate(90, 10));
    p5.setUserData(new Tuple2<>(4, "D"));
    points.add(p1);
    points.add(p2);
    points.add(p3);
    points.add(p4);
    points.add(p5);

    polygons.add((Polygon) reader.read("Polygon ((0 5,5 10,10 5,5 0,0 5))"));
    polygons.add((Polygon) reader.read("Polygon ((5 5,10 10,15 5,10 0,5 5))"));
  }

  @Test
  public void insertRemovePointTest() {
    TreeIndex<Point> treeIndex = new STRTreeIndex<>();
    treeIndex.insert(points);
    assertEquals(5, treeIndex.size());

    List<Point> r = treeIndex.query(points.get(3));
    assertEquals(2, r.size());

    treeIndex.remove(points.get(3));
    r = treeIndex.query(points.get(3));
    assertEquals(1, r.size());
  }

  @Test
  public void insertRemovePolygonTest() {
    TreeIndex<Polygon> treeIndex = new STRTreeIndex<>();
    treeIndex.insert(polygons);
    assertEquals(2, treeIndex.size());
    treeIndex.remove(polygons.get(0));
    assertEquals(1, treeIndex.size());
  }

  @Test
  public void pointQueryPolygonTest() {
    TreeIndex<Polygon> polygonIndex = new STRTreeIndex<>();
    polygonIndex.insert(polygons);

    Point p1 = factory.createPoint(new Coordinate(7, 3));
    Point p2 = factory.createPoint(new Coordinate(7, 2));
    Point p3 = factory.createPoint(new Coordinate(2, 1));
    Point p4 = factory.createPoint(new Coordinate(-1, 0));

    List<Polygon> r1 = polygonIndex.query(p1);
    System.out.println(r1);
    assertEquals(2, r1.size());
    List<Polygon> r2 = polygonIndex.query(p2);
    assertEquals(2, r2.size());
    List<Polygon> r3 = polygonIndex.query(p3);
    assertEquals(1, r3.size());
    List<Polygon> r4 = polygonIndex.query(p4);
    assertEquals(0, r4.size());
  }

  @Test
  public void rangeQueryPointTest() {
    TreeIndex<Point> treeIndex = new STRTreeIndex<>();
    treeIndex.insert(points);

    List<Point> result = treeIndex.query(new Envelope(110, 120, 30, 40));
    assertEquals(2, result.size());
  }

  @Test
  public void rangeQueryPolygonTest() {
    TreeIndex<Polygon> polygonTreeIndex = new STRTreeIndex<>();
    polygonTreeIndex.insert(polygons);

    List<Polygon> r1 = polygonTreeIndex.query(new Envelope(14, 20, 9, 12));
    assertEquals(1, r1.size());

    List<Polygon> r2 = polygonTreeIndex.query(new Envelope(5, 10, 0, 10));
    assertEquals(2, r2.size());
  }
}