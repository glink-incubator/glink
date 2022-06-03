package cn.edu.whu.glink.core.index;

import cn.edu.whu.glink.core.distance.DistanceCalculator;
import org.apache.flink.api.java.tuple.Tuple;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Objects;

/**
 * @author Yu Liebing
 */
public interface TreeIndexWithTime<T extends Geometry> {

    class Node<T extends Geometry> {
        T geom;
        Tuple attr;

        public Node(T geom) {
            this.geom = geom;
            this.attr = (Tuple) geom.getUserData();
        }

        public T getGeom() {
            return geom;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?> node = (Node<?>) o;
            return Objects.equals(attr, node.attr);
        }

        @Override
        public int hashCode() {
            return attr.hashCode();
        }

        @Override
        public String toString() {
            return "Node{geom=" + geom + ", attr=" + attr + '}';
        }
    }

    void insert(List<T> geometries, long ts);

    void insert(T geom, long ts);

    /**
     * Query with a bounding box.
     *
     * @param envelope query bounding box.
     *
     * @return all geometries in the tree which intersects with the query envelope.
     */
    List<T> query(Envelope envelope, long l, long u);

    /**
     * Query with a geometry.
     *
     * @param geometry query geometry
     *
     * @return all geometries in the tree which intersects with the query geometry.
     */
    List<T> query(Geometry geometry, long l, long u);

    /**
     * Query with the given distance. If the geometry is not a point,
     * the distance is the distance from the center of the geometry.
     *
     * @param geom     the query geometry
     * @param distance the query distance
     *
     * @return all geometries in the tree which within the distance will be return.
     */
    List<T> query(Geometry geom, long l, long u, double distance, DistanceCalculator calculator);

    void remove(T geom, long ts);

    int size();
}
