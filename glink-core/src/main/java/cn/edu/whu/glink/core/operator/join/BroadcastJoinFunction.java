package cn.edu.whu.glink.core.operator.join;

import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.geom.MultiPolygonWithIndex;
import cn.edu.whu.glink.core.geom.PolygonWithIndex;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.index.RTreeIndex;
import cn.edu.whu.glink.core.index.TreeIndex;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.util.List;

/**
 * @author Yu Liebing
 * */
public class BroadcastJoinFunction<T extends Geometry, T2 extends Geometry, OUT>
        extends BroadcastProcessFunction<T, Tuple2<Boolean, T2>, OUT> {

  private final TopologyType joinType;
  private final JoinFunction<T, T2, OUT> joinFunction;
  private MapStateDescriptor<Integer, TreeIndex<T2>> broadcastDesc;

  public BroadcastJoinFunction(TopologyType joinType, JoinFunction<T, T2, OUT> joinFunction) {
    this.joinType = joinType;
    this.joinFunction = joinFunction;
  }

  public void setBroadcastDesc(MapStateDescriptor<Integer, TreeIndex<T2>> broadcastDesc) {
    this.broadcastDesc = broadcastDesc;
  }

  @Override
  public void processElement(T t, ReadOnlyContext readOnlyContext, Collector<OUT> collector) throws Exception {
    ReadOnlyBroadcastState<Integer, TreeIndex<T2>> state = readOnlyContext.getBroadcastState(broadcastDesc);
    if (state.contains(0)) {
      TreeIndex<T2> treeIndex = state.get(0);
      if (joinType == TopologyType.WITHIN_DISTANCE) {
        List<T2> result = treeIndex.query(t, joinType.getDistance(), SpatialDataStream.distanceCalculator);
        for (T2 t2 : result) {
          collector.collect(joinFunction.join(t, t2));
        }
      } else {
        List<T2> result = treeIndex.query(t.getEnvelopeInternal());
        if (result.size() == 0) {
          collector.collect(joinFunction.join(t, null));
        }
        for (T2 t2 : result) {
          if (joinType == TopologyType.INTERSECTS) {
            collector.collect(joinFunction.join(t, t2));
          } else if (joinType == TopologyType.P_CONTAINS) {
            if (t.contains(t2)) collector.collect(joinFunction.join(t, t2));
          } else if (joinType == TopologyType.N_CONTAINS) {
            if (t2.contains(t)) collector.collect(joinFunction.join(t, t2));
          }
        }
      }
    }
  }

  @Override
  public void processBroadcastElement(Tuple2<Boolean, T2> t2, Context context, Collector<OUT> collector) throws Exception {
    BroadcastState<Integer, TreeIndex<T2>> state = context.getBroadcastState(broadcastDesc);
    if (!state.contains(0)) {
      state.put(0, new RTreeIndex<>());
    }
    TreeIndex<T2> treeIndex = state.get(0);
    if (t2.f0) {
      if (t2.f1 instanceof Polygon) {
        treeIndex.insert((T2) PolygonWithIndex.fromPolygon((Polygon) t2.f1));
      } else if (t2.f1 instanceof MultiPolygon) {
        treeIndex.insert((T2) MultiPolygonWithIndex.fromMultiPolygon((MultiPolygon) t2.f1));
      } else {
        treeIndex.insert(t2.f1);
      }
    } else {
      treeIndex.remove(t2.f1);
    }
  }
}
