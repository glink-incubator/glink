package cn.edu.whu.glink.core.process;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.operator.grid.GeometryDistributedGridMapper;
import cn.edu.whu.glink.core.operator.grid.GeometryGridMapper;
import cn.edu.whu.glink.core.operator.join.JoinWithTopologyType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 * */
public class SpatialIntervalJoin {

  public static <T1 extends Geometry, T2 extends Geometry> DataStream<Tuple2<T1, T2>> join(
          SpatialDataStream<T1> leftStream,
          SpatialDataStream<T2> rightStream,
          TopologyType joinType,
          Time lowerBound,
          Time upperBound) {
    DataStream<Tuple2<Long, T1>> stream1 =
            leftStream.getDataStream().flatMap(new GeometryGridMapper<>());
    DataStream<Tuple2<Long, T2>> stream2 =
            rightStream.getDataStream().flatMap(new GeometryDistributedGridMapper<>(joinType));

    return stream1
            .keyBy(t -> t.f0)
            .intervalJoin(stream2.keyBy(t -> t.f0))
            .between(lowerBound, upperBound)
            .process(new ProcessJoinFunction<Tuple2<Long, T1>, Tuple2<Long, T2>, Tuple2<T1, T2>>() {

              @Override
              public void processElement(Tuple2<Long, T1> t1,
                                         Tuple2<Long, T2> t2,
                                         Context context, Collector<Tuple2<T1, T2>> collector) throws Exception {
                JoinWithTopologyType
                        .join(t1.f1, t2.f1, joinType, Tuple2::new, SpatialDataStream.distanceCalculator)
                        .ifPresent(collector::collect);
              }
            });
  }
}
