package cn.edu.whu.glink.core.operator.join;

import cn.edu.whu.glink.core.geom.Circle;
import cn.edu.whu.glink.core.distance.DistanceCalculator;
import cn.edu.whu.glink.core.index.RTreeIndex;
import cn.edu.whu.glink.core.index.TreeIndex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 * */
public class BroadcastKNNJoinFunction<T extends Geometry, T2 extends Geometry, OUT>
        extends BroadcastProcessFunction<Tuple2<Long, T>, Tuple2<Boolean, T2>, Tuple4<Long, Double, T, OUT>> {

  private final double distance;
  private final MapFunction<T2, OUT> mapFunction;
  private MapStateDescriptor<Integer, TreeIndex<Circle>> broadcastDesc;
  private DistanceCalculator distanceCalculator;

  public BroadcastKNNJoinFunction(double distance,
                                  MapFunction<T2, OUT> mapFunction,
                                  DistanceCalculator calculator) {
    this.distance = distance;
    this.mapFunction = mapFunction;
    this.distanceCalculator = calculator;
  }

  public void setBroadcastDesc(MapStateDescriptor<Integer, TreeIndex<Circle>> broadcastDesc) {
    this.broadcastDesc = broadcastDesc;
  }

  @Override
  public void processElement(Tuple2<Long, T> t,
                             ReadOnlyContext readOnlyContext,
                             Collector<Tuple4<Long, Double, T, OUT>> collector) throws Exception {
    ReadOnlyBroadcastState<Integer, TreeIndex<Circle>> state = readOnlyContext.getBroadcastState(broadcastDesc);
    if (state.contains(0)) {
      TreeIndex<Circle> treeIndex = state.get(0);
      List<Circle> result = treeIndex.query(t.f1);
      for (Circle r : result) {
        if (r.contains(t.f1)) {
          System.out.println(t);
          collector.collect(new Tuple4<>(t.f0, distanceCalculator.calcDistance(r, t.f1), t.f1, mapFunction.map((T2) r.getCenter())));
        }
      }
    }
  }

  @Override
  public void processBroadcastElement(Tuple2<Boolean, T2> t2, Context context, Collector<Tuple4<Long, Double, T, OUT>> collector) throws Exception {
    BroadcastState<Integer, TreeIndex<Circle>> state = context.getBroadcastState(broadcastDesc);
    if (!state.contains(0)) {
      state.put(0, new RTreeIndex<>());
    }
    TreeIndex<Circle> treeIndex = state.get(0);
    Circle circle = new Circle(t2.f1, distance, distanceCalculator);
    if (!t2.f0) {
      treeIndex.remove(circle);
    } else {
      treeIndex.insert(circle);
    }
  }
}
