package cn.edu.whu.glink.core.index;

import cn.edu.whu.glink.core.util.GeoUtils;
import org.locationtech.jts.geom.Envelope;

import java.util.List;

/**
 * Grid division based on geographic coordinate system.
 *
 * @author Yu Liebing
 */
public class GeographicalGridIndex extends GridIndex {

  {
    minX = -180.0;
    maxX = 180.0;
    minY = -90.0;
    maxY = 90.0;
  }

  public GeographicalGridIndex(final int res) {
    if (res <= 0 || res > MAX_BITS) {
      throw new IllegalArgumentException("Resolution of grid index must in [1, 30]");
    }
    this.res = res;
    int splits = (int) Math.pow(2, res);
    xDis = (maxX - minX) / splits;
    yDis = (maxY - minY) / splits;
  }

  public GeographicalGridIndex(final Envelope envelope,
                               final double lngDistance,
                               final double latDistance) {
    this.minX = envelope.getMinX();
    this.maxX = envelope.getMaxX();
    this.minY = envelope.getMinY();
    this.maxY = envelope.getMaxY();
    xDis = GeoUtils.distanceToDEG(lngDistance);
    yDis = GeoUtils.distanceToDEG(latDistance);
  }

  public GeographicalGridIndex(final Envelope envelope,
                               final int lngSplits,
                               final int latSplits) {
    this(
            envelope.getMinX(),
            envelope.getMaxX(),
            envelope.getMinY(),
            envelope.getMaxY(),
            lngSplits,
            latSplits);
  }

  public GeographicalGridIndex(final double minLng, final double maxLng,
                               final double minLat, final double maxLat,
                               final int lngSplits, final int latSplits) {
    this.minX = minLng;
    this.maxX = maxLng;
    this.minY = minLat;
    this.maxY = maxLat;
    xDis = (maxLng - minLng) / lngSplits;
    yDis = (maxLat - minLat) / latSplits;
  }

  public GeographicalGridIndex(final double gridDistance) {
    double deg = GeoUtils.distanceToDEG(gridDistance);
    xDis = deg;
    yDis = deg;
  }

  @Override
  public List<Long> getIndex(double x, double y, double distance) {
    Envelope envelope = GeoUtils.calcEnvelopeByDis(x, y, distance);
    return getIndex(envelope);
  }

  @Override
  public List<Long> getIndex(double x, double y, double distance, boolean reduce) {
    if (!reduce) {
      return getIndex(x, y, distance);
    }
    Envelope envelope = GeoUtils.calcEnvelopeByDis(x, y, distance);
    return getReduceIndex(x, y, envelope, distance);
  }
}
