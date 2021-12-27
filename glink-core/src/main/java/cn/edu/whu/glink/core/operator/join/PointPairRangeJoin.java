package cn.edu.whu.glink.core.operator.join;

import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.util.GeoUtils;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.index.GridIndex;
import cn.edu.whu.glink.core.index.RTreeIndex;
import cn.edu.whu.glink.core.index.TreeIndex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

/**
 * Point pair range join. Mainly use for analysis applications like DBSCAN.
 * */
public class PointPairRangeJoin {

  public static DataStream<Tuple2<Point2, Point2>> join(
          final SpatialDataStream<Point2> pointDataStream,
          final double distance,
          final WindowAssigner<Object, TimeWindow> assigner) {
    GridIndex gridIndex = SpatialDataStream.gridIndex;

    DataStream<Tuple3<Boolean, Long, Point2>> redundantStream =
            pointDataStream.getDataStream().flatMap(new FlatMapFunction<Point2, Tuple3<Boolean, Long, Point2>>() {
              @Override
              public void flatMap(Point2 point, Collector<Tuple3<Boolean, Long, Point2>> collector) throws Exception {
                long index = gridIndex.getIndex(point.getX(), point.getY());
                collector.collect(new Tuple3<>(false, index, point));

                List<Long> indices = gridIndex.getIndex(point.getX(), point.getY(), distance, true);
                for (Long idx : indices) {
                  if (idx == index) {
                    continue;
                  }
                  collector.collect(new Tuple3<>(true, idx, point));
                }
              }
            });

    return redundantStream
            .keyBy(t -> t.f1)
            .window(assigner)
            .apply(new PairRangeJoinWindowFunction(distance));
  }

  public static class PairRangeJoinWindowFunction
          extends RichWindowFunction<Tuple3<Boolean, Long, Point2>, Tuple2<Point2, Point2>, Long, TimeWindow> {

    private final double distance;

    public PairRangeJoinWindowFunction(double distance) {
      this.distance = distance;
    }

    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<Tuple3<Boolean, Long, Point2>> iterable,
                      Collector<Tuple2<Point2, Point2>> collector) throws Exception {
      TreeIndex<Point2> treeIndex = new RTreeIndex<>();
      List<Point2> queryList = new LinkedList<>();
      for (Tuple3<Boolean, Long, Point2> t : iterable) {
        if (t.f0) {
          queryList.add(t.f2);
          continue;
        }
        List<Point2> queryResult = treeIndex.query(GeoUtils.calcEnvelopeByDis(t.f2, distance));
        for (Point2 p : queryResult) {
          collector.collect(new Tuple2<>(t.f2, p));
        }
        treeIndex.insert(t.f2);
      }

      for (Point2 p : queryList) {
        List<Point2> queryResult = treeIndex.query(GeoUtils.calcEnvelopeByDis(p, distance));
        for (Point2 pp : queryResult) {
          collector.collect(new Tuple2<>(p, pp));
        }
      }
    }
  }
}
