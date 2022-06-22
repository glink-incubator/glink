package cn.edu.whu.glink.core.operator.grid;

import cn.edu.whu.glink.core.tile.Pixel;
import cn.edu.whu.glink.core.tile.TileGrid;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Point;

/**
 * @author Xu Qi
 */
public class PixelGenerateMapper<T> extends
    RichFlatMapFunction<Tuple3<Pixel, T, String>, Tuple3<Pixel, T, String>> {
  private transient TileGrid[] tileGrids;
  int hLevel, lLevel, levelNum;

  public PixelGenerateMapper(int hLevel, int lLevel) {
    this.hLevel = hLevel;
    this.lLevel = lLevel;
    levelNum = hLevel - lLevel + 1;
  }

  @Override
  public void open(Configuration conf) {
    int length = hLevel - lLevel + 1;
    tileGrids = new TileGrid[length];
    int i = length;
    int j = hLevel;
    while (0 < i) {
      tileGrids[i - 1] = new TileGrid(j);
      i--;
      j--;
    }
  }

  @Override
  public void flatMap(Tuple3<Pixel, T, String> geom, Collector<Tuple3<Pixel, T, String>> collector) throws Exception {
    int i = levelNum;
    if (geom.f1 instanceof Point) {
      Point point = (Point) geom.f1;
      while (0 < i) {
        collector.collect(new Tuple3<>(tileGrids[i - 1].getPixel(point), geom.f1, geom.f2));
        i = i - 1;
      }
    } else {
      throw new IllegalArgumentException("Unsupported geom type");
    }
  }
}
