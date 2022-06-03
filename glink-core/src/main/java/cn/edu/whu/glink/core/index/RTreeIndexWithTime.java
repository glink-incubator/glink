package cn.edu.whu.glink.core.index;

import cn.edu.whu.glink.core.distance.DistanceCalculator;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import org.apache.flink.api.java.tuple.Tuple;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * 3D-R-Tree implementation borrow from
 * <a href="https://github.com/davidmoten/rtree-multi">https://github.com/davidmoten/rtree-multi</a>
 *
 * @author Lynn Lee
 */
public class RTreeIndexWithTime<T extends Geometry> implements TreeIndexWithTime<T> {

    private static final int DEFAULT_NODE_CAPACITY = 3;
    private static final int DEFAULT_NODE_DIMENSION = 3;

    private RTree<Node<T>, com.github.davidmoten.rtreemulti.geometry.Geometry> rTree;

    public RTreeIndexWithTime() {
        this(DEFAULT_NODE_CAPACITY, DEFAULT_NODE_DIMENSION);
    }

    public RTreeIndexWithTime(int nodeCapacity, int dimension) {
        if (nodeCapacity < 3) {
            throw new IllegalArgumentException("Node capacity of R-Tree must greater than 2");
        }
        if (dimension < 1) {
            throw new IllegalArgumentException("Node dimension of R-Tree must greater than 2");
        }
        rTree = RTree.dimensions(dimension).maxChildren(nodeCapacity).create();
    }

    @Override
    public void insert(List<T> geometries, long timestamp) {
        for (T geom : geometries) {
            insertGeom(geom, timestamp);
        }
    }

    public void insert(List<T> geometries, int timeIndex) {
        for (T geom : geometries) {
            insertGeom(geom, ((Tuple) geom.getUserData()).getField(timeIndex));
        }
    }

    @Override
    public void insert(T geom, long timestamp) {
        insertGeom(geom, timestamp);
    }


    public void insert(T geom, int timeIndex) {
        insertGeom(geom, ((Tuple) geom.getUserData()).getField(timeIndex));
    }

    @Override
    public List<T> query(Envelope envelope, long lowerTimestamp, long upperTimestamp) {
        List<T> result = new ArrayList<>();
        rTree.search(envelopeToRect(envelope, lowerTimestamp, upperTimestamp))
                .forEach(item -> result.add(item.value().geom));
        return result;
    }

    @Override
    public List<T> query(Geometry geometry, long lowerTimestamp, long upperTimestamp) {
        return query(geometry.getEnvelopeInternal(), lowerTimestamp, upperTimestamp);
    }

    @Override
    public List<T> query(Geometry geom, long lowerTimestamp, long upperTimestamp, double distance, DistanceCalculator calculator) {
        Point point = geom instanceof Point ? (Point) geom : geom.getCentroid();
        Envelope box = calculator.calcBoxByDist(point, distance);
        List<T> result = new ArrayList<>();
        rTree.search(envelopeToRect(box, lowerTimestamp, upperTimestamp))
                .forEach(item -> {
                    if (calculator.calcDistance(geom, item.value().geom) <= distance) {
                        result.add(item.value().geom);
                    }
                });
        return result;
    }

    @Override
    public void remove(T geom, long timestamp) {
        Envelope box = geom.getEnvelopeInternal();
        Node<T> node = new Node<>(geom);
        rTree = rTree.delete(node, envelopeToRect(box, timestamp, timestamp));
    }

    @Override
    public int size() {
        return rTree.size();
    }

    private void insertGeom(T geom, long timestamp) {
        Envelope box = geom.getEnvelopeInternal();
        Node<T> node = new Node<>(geom);
        rTree = rTree.add(node, envelopeToRect(box, timestamp, timestamp));
    }

    private static Rectangle envelopeToRect(Envelope box, long lowerTimestamp, long upperTimestamp) {
        double[] mins = {lowerTimestamp, box.getMinX(), box.getMinY()};
        double[] maxs = {upperTimestamp, box.getMaxX(), box.getMaxY()};
        return Rectangle.create(mins, maxs);
    }

    @Override
    public String toString() {
        return rTree.asString();
    }
}
