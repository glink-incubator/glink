package cn.edu.whu.glink.core.index;

import cn.edu.whu.glink.core.distance.DistanceCalculator;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * R-Tree implementation borrow from
 * <a href="https://github.com/davidmoten/rtree">https://github.com/davidmoten/rtree</a>
 *
 * @author Yu Liebing
 * */
public class RTreeIndex<T extends Geometry> implements TreeIndex<T> {

  private static final int DEFAULT_NODE_CAPACITY = 3;

  private RTree<Node<T>, com.github.davidmoten.rtree.geometry.Geometry> rTree;

  public RTreeIndex() {
    this(DEFAULT_NODE_CAPACITY);
  }

  public RTreeIndex(int nodeCapacity) {
    if (nodeCapacity < 3)
      throw new IllegalArgumentException("Node capacity of R-Tree must greater than 2");
    rTree = RTree.maxChildren(DEFAULT_NODE_CAPACITY).create();
  }

  @Override
  public void insert(List<T> geometries) {
    geometries.forEach(this::insertGeom);
  }

  @Override
  public void insert(T geom) {
    insertGeom(geom);
  }

  @Override
  public List<T> query(Envelope envelope) {
    List<T> result = new ArrayList<>();
    rTree.search(envelopeToRect(envelope))
            .toBlocking()
            .toIterable()
            .forEach(item -> result.add(item.value().geom));
    return result;
  }

  @Override
  public List<T> query(Geometry geometry) {
    return query(geometry.getEnvelopeInternal());
  }

  @Override
  public List<T> query(Geometry geom, double distance, DistanceCalculator calculator) {
    Point point = geom instanceof Point ? (Point) geom : geom.getCentroid();
    Envelope box = calculator.calcBoxByDist(point, distance);
    List<T> result = new ArrayList<>();
    rTree.search(envelopeToRect(box))
            .toBlocking()
            .toIterable()
            .forEach(item -> {
              if (calculator.calcDistance(geom, item.value().geom) <= distance)
                result.add(item.value().geom);
            });
    return result;
  }

  @Override
  public void remove(T geom) {
    Envelope box = geom.getEnvelopeInternal();
    Node<T> node = new Node<>(geom);
    rTree = rTree.delete(node, envelopeToRect(box));
  }

  @Override
  public int size() {
    return rTree.size();
  }

  private void insertGeom(T geom) {
    Envelope box = geom.getEnvelopeInternal();
    Node<T> node = new Node<>(geom);
    rTree = rTree.add(node, envelopeToRect(box));
  }

  private static Rectangle envelopeToRect(Envelope box) {
    return Geometries.rectangle(box.getMinX(), box.getMinY(), box.getMaxX(), box.getMaxY());
  }

  @Override
  public String toString() {
    return rTree.asString();
  }
}
