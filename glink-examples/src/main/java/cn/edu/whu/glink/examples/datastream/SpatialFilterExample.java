package cn.edu.whu.glink.examples.datastream;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.process.SpatialFilter;
import cn.edu.whu.glink.core.serialize.GlinkSerializerRegister;
import cn.edu.whu.glink.examples.utils.SimplePointFlatMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple example for showing how to perform spatial filter.
 *
 * <p>
 * Input line with format (id,lng,lat), below is an example:
 * 1,114.35,34.50
 *
 * @author Yu Liebing
 * */
public class SpatialFilterExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GlinkSerializerRegister.registerSerializer(env);

        WKTReader reader = SpatialDataStream.wktReader;
        List<Polygon> polygons = new ArrayList<>();
        polygons.add((Polygon) reader.read("Polygon ((0 5,5 10,10 5,5 0,0 5))"));
        polygons.add((Polygon) reader.read("Polygon ((5 5,10 10,15 5,10 0,5 5))"));

        SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
                env, "localhost", 9999, new SimplePointFlatMapper());
        DataStream<Point> resultStream = SpatialFilter.filter(pointDataStream, polygons, TopologyType.CONTAINS);
        resultStream.print();

        env.execute("Glink Spatial Filter Example");
    }
}
