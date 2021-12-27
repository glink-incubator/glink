package cn.edu.whu.glink.core.process;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.geom.MultiPolygonWithIndex;
import cn.edu.whu.glink.core.geom.PolygonWithIndex;
import cn.edu.whu.glink.core.index.STRTreeIndex;
import cn.edu.whu.glink.core.index.TreeIndex;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.operation.buffer.BufferOp;

import java.util.List;
import java.util.function.Predicate;

/**
 * The spatial filter class.
 *
 * @author Yu Liebing
 */
public class SpatialFilter {

  /**
   * To filter a spatial data stream with a geometry.
   *
   * @param spatialDataStream the spatial data stream need to be filtered.
   * @param geometry the filter geometry
   * @param topologyType type of filtering used to perform
   * */
  public static <T extends Geometry, U extends Geometry> DataStream<T> filter(
          SpatialDataStream<T> spatialDataStream,
          U geometry,
          TopologyType topologyType) {
    return spatialDataStream.getDataStream().filter(new RichFilterFunction<T>() {

      private transient Predicate<T> predicate;

      @Override
      public void open(Configuration parameters) throws Exception {
        Geometry filerGeometry = geometry;
        // optimize for Polygon/MultiPolygon.contains(Point)
        if (geometry instanceof Polygon) {
          filerGeometry = PolygonWithIndex.fromPolygon((Polygon) geometry);
        } else if (geometry instanceof MultiPolygon) {
          filerGeometry = MultiPolygonWithIndex.fromMultiPolygon((MultiPolygon) geometry);
        }
        Geometry finalFilerGeometry = filerGeometry;

        switch (topologyType) {
          case CONTAINS:
            predicate = finalFilerGeometry::contains;
            break;
          case CROSSES:
            predicate = finalFilerGeometry::crosses;
            break;
          case DISJOINT:
            predicate = finalFilerGeometry::disjoint;
            break;
          case EQUAL:
            predicate = finalFilerGeometry::equals;
            break;
          case INTERSECTS:
            predicate = finalFilerGeometry::intersects;
            break;
          case OVERLAPS:
            predicate = finalFilerGeometry::overlaps;
            break;
          case TOUCH:
            predicate = finalFilerGeometry::touches;
            break;
          case WITHIN:
            predicate = finalFilerGeometry::within;
            break;
          case WITHIN_DISTANCE:
          case BUFFER:
            final Geometry bufferGeometry = BufferOp.bufferOp(filerGeometry, topologyType.getDistance());
            predicate = bufferGeometry::contains;
            break;
          default:
            throw new IllegalArgumentException("Unsupported topology type");
        }
      }

      @Override
      public boolean filter(T g) throws Exception {
        return predicate.test(g);
      }
    });
  }

  public static <T extends Geometry, U extends Geometry> DataStream<T> filter(
          SpatialDataStream<T> spatialDataStream,
          List<U> geometries,
          TopologyType topologyType) {
    if (geometries.size() == 1) {
      return filter(spatialDataStream, geometries.get(0), topologyType);
    }
    return spatialDataStream.getDataStream().filter(new RichFilterFunction<T>() {

      private transient Predicate<T> predicate;

      @SuppressWarnings("checkstyle:TodoComment")
      @Override
      public void open(Configuration parameters) throws Exception {
        final TreeIndex<U> index = new STRTreeIndex<>();
        for (U geom : geometries) {
          if (geom instanceof Polygon) {
            index.insert((U) PolygonWithIndex.fromPolygon((Polygon) geom));
          } else if (geom instanceof MultiPolygon) {
            index.insert((U) MultiPolygonWithIndex.fromMultiPolygon((MultiPolygon) geom));
          } else {
            index.insert(geom);
          }
        }

        // TODO: support more spatial relations for multi query geometry
        switch (topologyType) {
          case CONTAINS:
            predicate = g -> {
              List<U> result = index.query(g);
              for (U queryGeom : result) {
                if (queryGeom.contains(g)) {
                  return true;
                }
              }
              return false;
            };
            break;
          default:
            throw new IllegalArgumentException("Unsupported topology type");
        }
      }

      @Override
      public boolean filter(T t) throws Exception {
        return predicate.test(t);
      }
    });
  }
}
