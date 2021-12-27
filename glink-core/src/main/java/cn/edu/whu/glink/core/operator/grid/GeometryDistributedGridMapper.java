package cn.edu.whu.glink.core.operator.grid;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.function.Function;

/**
 * @author Yu Liebing
 * */
public class GeometryDistributedGridMapper<T extends Geometry>
        extends RichFlatMapFunction<T, Tuple2<Long, T>> {

  private final TopologyType type;
  private transient Function<T, List<Long>> func;

  public GeometryDistributedGridMapper(TopologyType type) {
    this.type = type;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    if (type == TopologyType.WITHIN_DISTANCE) {
      func = (T g) -> {
        Envelope envelope = SpatialDataStream.distanceCalculator.calcBoxByDist(g, type.getDistance());
        return SpatialDataStream.gridIndex.getIndex(envelope);
      };
    } else {
      func = (T g) -> SpatialDataStream.gridIndex.getIndex(g);
    }
  }

  @Override
  public void flatMap(T geom, Collector<Tuple2<Long, T>> collector) throws Exception {
    List<Long> indices = func.apply(geom);
    for (Long index : indices) {
      collector.collect(new Tuple2<>(index, geom));
    }
  }
}
