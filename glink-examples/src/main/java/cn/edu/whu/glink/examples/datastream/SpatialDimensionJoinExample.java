package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.datastream.BroadcastSpatialDataStream;
import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.process.SpatialDimensionJoin;
import cn.edu.whu.glink.examples.utils.WKTGeometryBroadcastMapper;
import cn.edu.whu.glink.examples.utils.SimplePointFlatMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * A simple example of how to use glink to perform spatial dimension join.
 *
 * 1;POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))
 *
 * (POINT (15 15),POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10)))
 * (POINT (20 20),POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10)))
 * (POINT (21 21),null)
 *
 * @author Yu Liebing
 * */
public class SpatialDimensionJoinExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SpatialDataStream<Point> spatialDataStream1 = new SpatialDataStream<>(
            env, "localhost", 8888, new SimplePointFlatMapper());
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(
            env, "localhost", 9999, new WKTGeometryBroadcastMapper());

    SpatialDimensionJoin.join(
            spatialDataStream1,
            spatialDataStream2,
            TopologyType.N_CONTAINS,
            Tuple2::new,
            new TypeHint<Tuple2<Point, Geometry>>() { })
            .print();

    env.execute("Glink Spatial Dimension Join Example");
  }
}
