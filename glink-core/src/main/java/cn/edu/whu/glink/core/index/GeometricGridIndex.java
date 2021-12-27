package cn.edu.whu.glink.core.index;

import org.locationtech.jts.geom.Envelope;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class GeometricGridIndex extends GridIndex {

  public GeometricGridIndex(int res, Envelope box) {
    if (res < 0 || res > MAX_BITS)
      throw new IllegalArgumentException("Resolution of GridIndex must between [0, 30]");
    this.res = res;
    minX = box.getMinX();
    maxX = box.getMaxX();
    minY = box.getMinY();
    maxY = box.getMaxY();
    double splits = Math.pow(2, res);
    xDis = (maxX - minX) / splits;
    yDis = (maxY - minY) / splits;
  }

  public GeometricGridIndex(Envelope envelope, double xDistance, double yDistance) {
    this.minX = envelope.getMinX();
    this.maxX = envelope.getMaxX();
    this.minY = envelope.getMinY();
    this.maxY = envelope.getMaxY();
    xDis = xDistance;
    yDis = yDistance;
  }

  public GeometricGridIndex(double minX, double maxX,
                            double minY, double maxY,
                            int xSplits, int ySplits) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    xDis = (maxX - minX) / xSplits;
    yDis = (maxY - minY) / ySplits;
  }

  public GeometricGridIndex(Envelope envelope, int xSplits, int ySplits) {
    this(
            envelope.getMinX(),
            envelope.getMaxX(),
            envelope.getMinY(),
            envelope.getMaxY(),
            xSplits,
            ySplits);
  }

  @Override
  public List<Long> getIndex(double x, double y, double distance) {
    Envelope envelope = new Envelope(
            x - distance, x + distance,
            y - distance, y + distance);
    return getIndex(envelope);
  }

  @Override
  public List<Long> getIndex(double x, double y, double distance, boolean reduce) {
    if (!reduce) {
      return getIndex(x, y, distance);
    }
    Envelope envelope = new Envelope(
            x - distance, x + distance, y - distance, y + distance);
    return getReduceIndex(x, y, envelope, distance);
  }
}
