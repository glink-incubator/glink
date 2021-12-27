package cn.edu.whu.glink.core.operator.join;

import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.distance.DistanceCalculator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.locationtech.jts.geom.Geometry;

import java.util.Optional;

/**
 * @author Yu Liebing
 * */
public class JoinWithTopologyType {

  public static <IN1 extends Geometry, IN2 extends Geometry, OUT> Optional<OUT> join(
          IN1 geom1,
          IN2 geom2,
          TopologyType type,
          JoinFunction<IN1, IN2, OUT> joinFunction,
          DistanceCalculator distanceCalculator) throws Exception {
    switch (type) {
      case INTERSECTS:
        return geom1.intersects(geom2) ? Optional.of(joinFunction.join(geom1, geom2)) : Optional.empty();
      case P_CONTAINS:
        return geom1.contains(geom2) ? Optional.of(joinFunction.join(geom1, geom2)) : Optional.empty();
      case N_CONTAINS:
        return geom2.contains(geom1) ? Optional.of(joinFunction.join(geom1, geom2)) : Optional.empty();
      case P_WITHIN:
        return geom1.within(geom2) ? Optional.of(joinFunction.join(geom1, geom2)) : Optional.empty();
      case N_WITHIN:
        return geom2.within(geom1) ? Optional.of(joinFunction.join(geom1, geom2)) : Optional.empty();
      case WITHIN_DISTANCE:
        return distanceCalculator.calcDistance(geom1, geom2) <= type.getDistance()
                ? Optional.of(joinFunction.join(geom1, geom2)) : Optional.empty();
      case P_BUFFER:
        throw new IllegalArgumentException("Currently cannot do buffer join.");
      case N_BUFFER:
        throw new IllegalArgumentException("Currently cannot do buffer join.");
      default:
        throw new IllegalArgumentException("Illegal join type");
    }
  }
}
