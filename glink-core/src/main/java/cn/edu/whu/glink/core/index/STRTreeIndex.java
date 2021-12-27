package cn.edu.whu.glink.core.index;

import cn.edu.whu.glink.core.distance.DistanceCalculator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class STRTreeIndex<T extends Geometry> implements TreeIndex<T> {

  private static final int DEFAULT_NODE_CAPACITY = 2;
  private final STRtree stRtree;

  public STRTreeIndex() {
    stRtree = new STRtree(DEFAULT_NODE_CAPACITY);
  }

  public STRTreeIndex(int nodeCapacity) {
    stRtree = new STRtree(nodeCapacity);
  }

  @Override
  public void insert(List<T> geometries) {
    geometries.forEach(this::insert);
  }

  @Override
  public void insert(T geom) {
    stRtree.insert(geom.getEnvelopeInternal(), geom);
  }

  @Override
  public List<T> query(Envelope envelope) {
    return stRtree.query(envelope);
  }

  @Override
  public List<T> query(Geometry geometry) {
    return query(geometry.getEnvelopeInternal());
  }

  @Override
  public List<T> query(Geometry geometry, double distance, DistanceCalculator calculator) {
    Point point = geometry instanceof Point ? (Point) geometry : geometry.getCentroid();
    Envelope envelope = calculator.calcBoxByDist(point, distance);
    List<T> result = stRtree.query(envelope);
    result.removeIf(geom -> calculator.calcDistance(point, geom) > distance);
    return result;
  }

  /**
   * STRTree use `==` to just the equality of objects int the tree,
   * so only support for removing object with the same address.
   * */
  @Override
  public void remove(T geom) {
    stRtree.remove(geom.getEnvelopeInternal(), geom);
  }

  @Override
  public int size() {
    return stRtree.size();
  }

  @Override
  public String toString() {
    return stRtree.toString();
  }
}
