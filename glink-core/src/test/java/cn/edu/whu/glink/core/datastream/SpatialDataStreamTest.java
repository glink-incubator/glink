package cn.edu.whu.glink.core.datastream;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class SpatialDataStreamTest {

  @SuppressWarnings("checkstyle:MethodName")
  @Test
  public void CRSTransformExample() throws FactoryException, TransformException {
    String sourceEpsgCRSCode = "epsg:4326";
    String targetEpsgCRSCode = "epsg:3857";

    CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
    CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
    final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, true);

    GeometryFactory factory = new GeometryFactory();
    Point point = factory.createPoint(new Coordinate(12.3, 13.4));

    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000; ++i) {
      Point p = (Point) JTS.transform(point, transform);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);

    System.out.println(point);
  }

}