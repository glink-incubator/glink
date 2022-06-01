package cn.edu.whu.glink.core.operator.grid;

import cn.edu.whu.glink.core.tile.Tile;
import cn.edu.whu.glink.core.tile.TileResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Xu Qi
 */
public class AddUpperKeyMapper<V> extends
    RichFlatMapFunction<TileResult<V>, Tuple2<TileResult<V>, Tile>> {

  @Override
  public void flatMap(TileResult<V> tileResult,
                      Collector<Tuple2<TileResult<V>, Tile>> collector) throws Exception {
    collector.collect(new Tuple2<>(tileResult, tileResult.getTile().getUpperTile()));
  }
}

