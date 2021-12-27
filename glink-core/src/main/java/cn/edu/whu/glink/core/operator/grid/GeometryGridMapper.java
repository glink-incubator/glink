package cn.edu.whu.glink.core.operator.grid;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 * */
public class GeometryGridMapper<T extends Geometry>
        implements FlatMapFunction<T, Tuple2<Long, T>> {

  @Override
  public void flatMap(T geom, Collector<Tuple2<Long, T>> collector) throws Exception {
    List<Long> indices = SpatialDataStream.gridIndex.getIndex(geom);
    for (Long index : indices) {
      collector.collect(new Tuple2<>(index, geom));
    }
  }
}
