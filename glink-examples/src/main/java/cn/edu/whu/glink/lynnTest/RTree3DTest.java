package cn.edu.whu.glink.lynnTest;

import cn.edu.whu.glink.core.index.RTreeIndexWithTime;
import org.locationtech.jts.geom.Point;

/**
 * @author Lynn Lee
 **/
public class RTree3DTest {
    public static void main(String[] args) {
        RTreeIndexWithTime<Point> rtree = new RTreeIndexWithTime<>(3, 3);
        int bp = -1;
    }
}
