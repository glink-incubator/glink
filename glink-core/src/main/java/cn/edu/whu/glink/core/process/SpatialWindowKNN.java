package cn.edu.whu.glink.core.process;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Point;

import java.util.PriorityQueue;

/**
 * The spatial window knn class.
 *
 * @author Yu Liebing
 * */
public class SpatialWindowKNN {

  public static <T extends Point> DataStream<T> knn(
          SpatialDataStream<T> pointStream,
          Point queryPoint,
          int k,
          double maxDistance,
          WindowAssigner<Object, TimeWindow> assigner) {
    DataStream<Tuple2<Long, T>> indexStream = pointStream.getDataStream().flatMap(new PointIndexMapper<>());
    return indexStream
            .keyBy(t -> t.f0)
            .window(assigner)
            .apply(new FilterWindow<>(queryPoint, k, maxDistance))
            .windowAll(assigner)
            .apply(new MergeWindow<>(k));
  }

  public static class PointIndexMapper<T extends Point> implements FlatMapFunction<T, Tuple2<Long, T>> {

    @Override
    public void flatMap(T point, Collector<Tuple2<Long, T>> collector) throws Exception {
      long index = SpatialDataStream.gridIndex.getIndex(point);
      collector.collect(new Tuple2<>(index, point));
    }
  }

  public static class FilterWindow<T extends Point>
          implements WindowFunction<Tuple2<Long, T>, Tuple2<Double, T>, Long, TimeWindow> {

    private final Point queryPoint;
    private final int k;
    private final double maxDistance;

    public FilterWindow(Point queryPoint, int k, double maxDistance) {
      this.queryPoint = queryPoint;
      this.k = k;
      this.maxDistance = maxDistance;
    }

    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<Tuple2<Long, T>> iterable,
                      Collector<Tuple2<Double, T>> collector) throws Exception {
      PriorityQueue<Tuple2<Double, T>> pq = new PriorityQueue<>(512, (t1, t2) -> {
                if (Math.abs(t1.f0 - t2.f0) < 1e-6) return 0;
                else return (t2.f0 - t1.f0) > 0 ? 1 : -1; });
      for (Tuple2<Long, T> t : iterable) {
        double dis = SpatialDataStream.distanceCalculator.calcDistance(queryPoint, t.f1);
        if (dis > maxDistance) continue;
        pq.offer(new Tuple2<>(dis, t.f1));
        if (pq.size() > k) {
          pq.poll();
        }
      }
      for (Tuple2<Double, T> t : pq) {
        collector.collect(t);
      }
    }
  }

  public static class MergeWindow<T extends Point>
          implements AllWindowFunction<Tuple2<Double, T>, T, TimeWindow> {

    private final int k;

    public MergeWindow(int k) {
      this.k = k;
    }

    @Override
    public void apply(TimeWindow timeWindow,
                      Iterable<Tuple2<Double, T>> iterable,
                      Collector<T> collector) throws Exception {
      PriorityQueue<Tuple2<Double, T>> pq = new PriorityQueue<>(512, (t1, t2) -> {
        if (Math.abs(t1.f0 - t2.f0) < 1e-6) return 0;
        else return (t2.f0 - t1.f0) > 0 ? 1 : -1; });
      for (Tuple2<Double, T> t : iterable) {
        pq.offer(new Tuple2<>(t.f0, t.f1));
        if (pq.size() > k) {
          pq.poll();
        }
      }
      for (Tuple2<Double, T> t : pq) {
        collector.collect(t.f1);
      }
    }
  }
}
