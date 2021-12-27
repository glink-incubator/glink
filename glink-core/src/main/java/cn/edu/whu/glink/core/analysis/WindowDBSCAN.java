package cn.edu.whu.glink.core.analysis;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.geom.Point2;
import cn.edu.whu.glink.core.operator.join.PointPairRangeJoin;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Process DBSCAN on windowed snapshots. It is done by the following steps:
 *
 * <p>1.Get pair neighbor stream using {@link PointPairRangeJoin}.
 * <p>2.Process DBSCAN with neighbour pairs.
 *
 * @author Yu Liebing
 */
public class WindowDBSCAN {

  public static DataStream<Tuple2<Integer, List<Point2>>> dbscan(
          SpatialDataStream<Point2> pointDataStream,
          double distance,
          int minPts,
          WindowAssigner<Object, TimeWindow> assigner) {
    DataStream<Tuple2<Point2, Point2>> pairJoinStream = PointPairRangeJoin.join(
            pointDataStream, distance, assigner);

    return pairJoinStream
            .windowAll(assigner)
            .apply(new DBSCANWindowFunction(minPts));
  }

  public static class DBSCANWindowFunction implements
          AllWindowFunction<Tuple2<Point2, Point2>, Tuple2<Integer, List<Point2>>, TimeWindow> {

    private final int minPts;

    public DBSCANWindowFunction(int minPts) {
      this.minPts = minPts;
    }

    @Override
    public void apply(TimeWindow timeWindow,
                      Iterable<Tuple2<Point2, Point2>> iterable,
                      Collector<Tuple2<Integer, List<Point2>>> collector) throws Exception {
      // build neighbour graph
      Map<Point2, List<Point2>> graph = new HashMap<>(512);
      for (Tuple2<Point2, Point2> t : iterable) {
        graph.putIfAbsent(t.f0, new LinkedList<>());
        graph.putIfAbsent(t.f1, new LinkedList<>());
        graph.get(t.f0).add(t.f1);
        graph.get(t.f1).add(t.f0);
      }
      Set<Point2> visited = new HashSet<>(512);
      int clusterID = 0;
      for (Map.Entry<Point2, List<Point2>> e : graph.entrySet()) {
        if (!visited.contains(e.getKey())) {
          if (e.getValue().size() >= minPts) {
            List<Point2> cluster = new LinkedList<>();
            Queue<Point2> q = new LinkedList<>();
            q.offer(e.getKey());
            visited.add(e.getKey());
            while (!q.isEmpty()) {
              Point2 p = q.poll();
              cluster.add(p);
              if (graph.get(p).size() >= minPts) {
                for (Point2 pn : graph.get(p)) {
                  if (!visited.contains(pn)) {
                    q.offer(pn);
                    visited.add(pn);
                  }
                }
              }
            }
            collector.collect(new Tuple2<>(clusterID, cluster));
            ++clusterID;
          }
        }
      }
    }
  }
}
