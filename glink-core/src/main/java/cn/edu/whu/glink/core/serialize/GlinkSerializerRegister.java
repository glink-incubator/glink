package cn.edu.whu.glink.core.serialize;

import cn.edu.whu.glink.core.geom.Circle;
import cn.edu.whu.glink.core.geom.Point2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.*;

/**
 * @author Yu Liebing
 */
public class GlinkSerializerRegister {

  public static void registerSerializer(StreamExecutionEnvironment env) {
    env.getConfig().registerTypeWithKryoSerializer(Point.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(Point2.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(MultiPolygon.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(LineString.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(MultiLineString.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(Polygon.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(MultiPolygon.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(GeometryCollection.class, GeometryKryoSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(Circle.class, GeometryKryoSerializer.class);
  }
}
