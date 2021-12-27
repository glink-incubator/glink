package cn.edu.whu.glink.core.process;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.index.STRTreeIndex;
import cn.edu.whu.glink.core.index.TreeIndex;
import cn.edu.whu.glink.core.operator.grid.GeometryDistributedGridMapper;
import cn.edu.whu.glink.core.operator.grid.GeometryGridMapper;
import cn.edu.whu.glink.core.operator.join.JoinWithTopologyType;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * The spatial window join class.
 *
 * @author Yu Liebing
 * */
public class SpatialWindowJoin {

  /**
   * Spatial window join of two {@link SpatialDataStream}
   * */
  public static <T1 extends Geometry, T2 extends Geometry, W extends Window> DataStream<Tuple2<T1, T2>> join(
          SpatialDataStream<T1> leftStream,
          SpatialDataStream<T2> rightStream,
          TopologyType joinType,
          WindowAssigner<? super CoGroupedStreams.TaggedUnion<Tuple2<Long, T1>, Tuple2<Long, T2>>, W> assigner) {
    DataStream<Tuple2<Long, T1>> stream1 =
            leftStream.getDataStream().flatMap(new GeometryGridMapper<>());
    DataStream<Tuple2<Long, T2>> stream2 =
            rightStream.getDataStream().flatMap(new GeometryDistributedGridMapper<>(joinType));

    return stream1.coGroup(stream2)
            .where(t1 -> t1.f0).equalTo(t2 -> t2.f0)
            .window(assigner)
            .apply(new SpatialWindowJoinFunction<>(joinType));
  }

  /**
   * Window join process function.
   * */
  public static class SpatialWindowJoinFunction<T1 extends Geometry, T2 extends Geometry>
          implements CoGroupFunction<Tuple2<Long, T1>, Tuple2<Long, T2>, Tuple2<T1, T2>> {

    private final TopologyType joinType;
    private final JoinFunction<T1, T2, Tuple2<T1, T2>> joinFunction;

    public SpatialWindowJoinFunction(TopologyType joinType) {
      this.joinType = joinType;
      this.joinFunction = Tuple2::new;
    }

    @Override
    public void coGroup(Iterable<Tuple2<Long, T1>> stream1,
                        Iterable<Tuple2<Long, T2>> stream2,
                        Collector<Tuple2<T1, T2>> collector) throws Exception {
      TreeIndex<T2> treeIndex = new STRTreeIndex<>();
      stream2.forEach(t2 -> treeIndex.insert(t2.f1));
      if (joinType == TopologyType.WITHIN_DISTANCE) {
        for (Tuple2<Long, T1> t1 : stream1) {
          List<T2> result = treeIndex.query(t1.f1, joinType.getDistance(), SpatialDataStream.distanceCalculator);
          for (T2 r : result) {
            collector.collect(joinFunction.join(t1.f1, r));
          }
        }
      } else {
        for (Tuple2<Long, T1> t1 :stream1) {
          List<T2> result = treeIndex.query(t1.f1);
          for (T2 r : result) {
            JoinWithTopologyType
                    .join(t1.f1, r, joinType, joinFunction, SpatialDataStream.distanceCalculator)
                    .ifPresent(collector::collect);
          }
        }
      }
    }
  }
}
