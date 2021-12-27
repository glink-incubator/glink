package cn.edu.whu.glink.connector.geomesa.util;

import org.locationtech.jts.io.ParseException;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

public class GeomesaTableSchemaTest {
  public static void main(String[] args) throws ParseException {
//    WKTReader wktReader = new WKTReader();
//    WKTWriter wktWriter = new WKTWriter();
//    Polygon polygon = (Polygon) wktReader.read("POLYGON ((30 10, 30 20, 40 20, 40 10, 30 10))");
//    GeometryFactory factory = new GeometryFactory();
//    Point point = factory.createPoint(new Coordinate(35, 15));
//    System.out.println(wktWriter.write(point));
//    System.out.println(polygon.contains(point));

    SpatialContextFactory factory = new SpatialContextFactory();
    SpatialContext spatialContext = new SpatialContext(factory);
    Point p1 = spatialContext.makePoint(10, 10);
    Point p2 = spatialContext.makePoint(7, 7);
    double dis = spatialContext.calcDistance(p1, p2) * DistanceUtils.DEG_TO_KM;
    System.out.println(dis);
  }
}