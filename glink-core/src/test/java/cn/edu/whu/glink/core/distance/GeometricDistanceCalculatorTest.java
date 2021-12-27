package cn.edu.whu.glink.core.distance;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class GeometricDistanceCalculatorTest {

  private DistanceCalculator calculator = new GeometricDistanceCalculator();
  private GeometryFactory factory = new GeometryFactory();

  @Test
  public void calcDistanceTest() {
    Point p1 = factory.createPoint(new Coordinate(0, 0));
    Point p2 = factory.createPoint(new Coordinate(3, 4));
    Assert.assertEquals(calculator.calcDistance(p1, p2), 5, 10e-6);
  }

  @Test
  public void calcBoxByDistTest() {
    Point p = factory.createPoint(new Coordinate(0, 0));
    Envelope envelope = calculator.calcBoxByDist(p, 12);
    System.out.println(envelope);
  }
}