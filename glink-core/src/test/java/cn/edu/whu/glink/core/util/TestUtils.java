package cn.edu.whu.glink.core.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.util.Random;

/**
 * Helper functions for tests.
 *
 * @author Yu Liebing
 * */
public class TestUtils {

  private static final Random RANDOM = new Random();

  public static Coordinate randomCoordinate(Envelope envelope) {
    double minX = envelope.getMinX(), maxX = envelope.getMaxX();
    double minY = envelope.getMinY(), maxY = envelope.getMaxY();
    double x = minX + RANDOM.nextDouble() * (maxX - minX);
    double y = minY + RANDOM.nextDouble() * (maxY - minY);
    return new Coordinate(x, y);
  }
}
