package cn.edu.whu.glink.core.operator.grid;

import cn.edu.whu.glink.core.datastream.TileGridDataStream;
import cn.edu.whu.glink.core.enums.PyramidTileAggregateType;
import cn.edu.whu.glink.core.enums.SmoothOperatorType;
import cn.edu.whu.glink.core.enums.TileFlatMapType;
import cn.edu.whu.glink.core.tile.Pixel;
import cn.edu.whu.glink.core.tile.PixelResult;
import cn.edu.whu.glink.core.tile.Tile;
import cn.edu.whu.glink.core.tile.TileResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Xu Qi
 */
public class WindowAggeFunction {
  public static class AddWindowTime<V, W extends TimeWindow> extends
      ProcessWindowFunction<TileResult<V>, TileResult<V>, Tile, W> {

    private static final long serialVersionUID = -1308201162807418668L;

    @Override
    public void process(Tile tile, Context context, Iterable<TileResult<V>> elements,
                        Collector<TileResult<V>> out) throws Exception {
      TileResult<V> tileResult = elements.iterator().next();
      tileResult.setTimeStart(new Timestamp(context.window().getStart()));
      tileResult.setTimeEnd(new Timestamp(context.window().getEnd()));
      out.collect(tileResult);
    }
  }

  public static class WindowAggregate<T extends Geometry, V>
      implements AggregateFunction<Tuple2<Pixel, T>,
      Map<Pixel, Tuple3<Double, HashSet<String>, Integer>>, TileResult<V>> {

    private final TileFlatMapType tileFlatMapType;
    private final SmoothOperatorType smoothOperator;
    private final int carIDIndex;
    private final int weightIndex;
    Double weight;

    public WindowAggregate(TileFlatMapType tileFlatMapType, SmoothOperatorType smoothOperator,int carIDIndex, int weightIndex) {
      this.tileFlatMapType = tileFlatMapType;
      this.smoothOperator = smoothOperator;
      this.carIDIndex = carIDIndex;
      this.weightIndex = weightIndex;
    }

    @Override
    public Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> add(
        Tuple2<Pixel, T> inPixel, Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> pixelIntegerMap) {
      Pixel pixel = inPixel.f0;
      String carNo = ((Tuple) inPixel.f1.getUserData()).getField(carIDIndex);
      if (weightIndex >= 0) {
        weight = Double.parseDouble(((Tuple) inPixel.f1.getUserData()).getField(weightIndex));
      } else {
        weight = 1.0;
      }
      try {
        if (!pixelIntegerMap.containsKey(pixel)) {
          HashSet<String> carNos = new HashSet<>();
          carNos.add(carNo);
          pixelIntegerMap.put(pixel, new Tuple3<>(weight, carNos, 1));
        } else if (!pixelIntegerMap.get(pixel).f1.contains(carNo)) {
          pixelIntegerMap.get(pixel).f1.add(carNo);
          switch (tileFlatMapType) {
            case MAX:
              pixelIntegerMap.get(pixel).f0 = Math.max(pixelIntegerMap.get(pixel).f0, weight);
              break;
            case MIN:
              pixelIntegerMap.get(pixel).f0 = Math.min(pixelIntegerMap.get(pixel).f0, weight);
              break;
            case COUNT:
              pixelIntegerMap.get(pixel).f0 = 1.0;
              break;
            case AVG:
            case SUM:
              pixelIntegerMap.get(pixel).f0 = pixelIntegerMap.get(pixel).f0 + weight;
              pixelIntegerMap.get(pixel).f2++;
              break;
            default:
              throw new IllegalArgumentException("Illegal tileFlatMap type");
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return pixelIntegerMap;
    }

    @Override
    public TileResult<V> getResult(Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> pixelIntegerMap) {
      TileResult<V> ret = new TileResult<>();
      Map<Pixel, Double> temple = new HashMap<>();
      ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
      for (Map.Entry<Pixel, Tuple3<Double, HashSet<String>, Integer>> entry : pixelIntegerMap.entrySet()) {
        double finalValue;
        if (tileFlatMapType == TileFlatMapType.AVG) {
          finalValue = entry.getValue().f0 / entry.getValue().f2;
        } else {
          finalValue = entry.getValue().f0;
        }
        temple.put(entry.getKey(), finalValue);
      }
      if (smoothOperator != null) {
        Map<Tuple2<Integer, Integer>, Double> mapOperator = SmoothOperatorType.
            getMapOperator(smoothOperator);
        Double defineValue = 0.0;
        for (Map.Entry<Pixel, Double> pixelDoubleEntry : temple.entrySet()) {
          if (pixelDoubleEntry.getKey().getPixelX() >= (SmoothOperatorType.getLength() / 2)
              && (pixelDoubleEntry.getKey().getPixelX() <= 255 - (SmoothOperatorType.getLength() / 2))
              && (pixelDoubleEntry.getKey().getPixelY() >= (SmoothOperatorType.getLength() / 2))
              && (pixelDoubleEntry.getKey().getPixelY() <= 255 - (SmoothOperatorType.getLength() / 2))) {
            for (Map.Entry<Tuple2<Integer, Integer>, Double> doubleEntry : mapOperator.entrySet()) {
              int modelX = pixelDoubleEntry.getKey().getPixelX() + doubleEntry.getKey().f0;
              int modelY = pixelDoubleEntry.getKey().getPixelY() + doubleEntry.getKey().f1;
              double modelValue = 0.0;
              for (Map.Entry<Pixel, Double> smooth : temple.entrySet()) {
                if (smooth.getKey().getPixelNo() == modelX + modelY * 256) {
                  modelValue = smooth.getValue();
                }
              }
              defineValue += modelValue * doubleEntry.getValue();
            }
            ret.addPixelResult(new PixelResult<>(pixelDoubleEntry.getKey(), (V) defineValue));
            defineValue = 0.0;
          } else {
            ret.addPixelResult(new PixelResult<>(pixelDoubleEntry.getKey(), (V) pixelDoubleEntry.getValue()));
          }
        }
      } else {
        for (Map.Entry<Pixel, Double> entry : temple.entrySet()) {
          ret.addPixelResult(new PixelResult<>(entry.getKey(), (V) entry.getValue()));
        }
      }
      return ret;
    }

    @Override
    public Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> merge(
        Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> acc0,
        Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> acc1) {
      Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> acc2 = new HashMap<>(acc0);
      acc1.forEach((key, value) -> acc2.merge(key, value, (v1, v2) ->
          new Tuple3<>(v1.f0 + v1.f0, combineSets(v1.f1, v2.f1), v1.f2 + v1.f2)));
      return acc2;
    }

    private HashSet<String> combineSets(HashSet<String> v1, HashSet<String> v2) {
      v1.addAll(v2);
      return v1;
    }
  }

  public static class LevelUpAggregate<T extends Geometry, V> implements
      AggregateFunction<Tuple2<TileResult<V>, Tile>, Map<Pixel, Double>, TileResult<V>> {

    private final TileFlatMapType tileFlatMapType;
    private final PyramidTileAggregateType pyramidTileAggregateType;

    public LevelUpAggregate(TileFlatMapType tileFlatMapType,
                            PyramidTileAggregateType pyramidTileAggregateType) {
      this.tileFlatMapType = tileFlatMapType;
      this.pyramidTileAggregateType = pyramidTileAggregateType;
    }

    @Override
    public Map<Pixel, Double> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<Pixel, Double> add(Tuple2<TileResult<V>, Tile> tileResultTileTuple2,
                                  Map<Pixel, Double> pixelIntegerMap) {
      Tile lowTile = tileResultTileTuple2.f0.getTile();
      Tile upperTile = tileResultTileTuple2.f1;
      int length = tileResultTileTuple2.f0.getGridResultList().size();
      int newPixelNo = 0, newPixelX = 0, newPixelY = 0;
      for (int i = 0; i < length; i++) {
        PixelResult<V> pixelValue = tileResultTileTuple2.f0.getGridResultList().get(i);
        int pixelY = pixelValue.getPixel().getPixelY();
        int pixelX = pixelValue.getPixel().getPixelX();
        if (((double) lowTile.getY() / 2) - upperTile.getY() == 0 &
            ((double) lowTile.getX() / 2 - upperTile.getX() == 0)) {
          newPixelX = pixelX / 2;
          newPixelY = pixelY / 2;
        } else if (((double) lowTile.getY() / 2) - upperTile.getY() > 0 &
            ((double) lowTile.getX() / 2 - upperTile.getX() == 0)) {
          newPixelX = pixelX / 2;
          newPixelY = pixelY / 2 + 128;
        } else if (((double) lowTile.getY() / 2) - upperTile.getY() == 0 &
            ((double) lowTile.getX() / 2 - upperTile.getX() > 0)) {
          newPixelX = pixelX / 2 + 128;
          newPixelY = pixelY / 2;
        } else if (((double) lowTile.getY() / 2) - upperTile.getY() > 0 &
            ((double) lowTile.getX() / 2 - upperTile.getX() > 0)) {
          newPixelX = pixelX / 2 + 128;
          newPixelY = pixelY / 2 + 128;
        }
        newPixelNo = newPixelY * 256 + newPixelX;
        Double value = (Double) pixelValue.getResult();
        Pixel newPixel = new Pixel(upperTile, newPixelX, newPixelY, newPixelNo);
        if (!pixelIntegerMap.containsKey(newPixel)) {
          pixelIntegerMap.put(newPixel, value);
        } else {
          if (pyramidTileAggregateType != null) {
            switch (pyramidTileAggregateType) {
              case MIN:
                pixelIntegerMap.put(newPixel, Math.min(pixelIntegerMap.get(newPixel), value));
                break;
              case MAX:
                pixelIntegerMap.put(newPixel, Math.max(pixelIntegerMap.get(newPixel), value));
                break;
              case AVG:
              case SUM:
                pixelIntegerMap.put(newPixel, pixelIntegerMap.get(newPixel) + value);
                break;
              case COUNT:
                pixelIntegerMap.put(newPixel, 1.0);
                break;
              default:
                throw new IllegalArgumentException("Illegal PyramidTileAggre Type");
            }
          } else {
            switch (tileFlatMapType) {
              case MIN:
                pixelIntegerMap.put(newPixel, Math.min(pixelIntegerMap.get(newPixel), value));
                break;
              case MAX:
                pixelIntegerMap.put(newPixel, Math.max(pixelIntegerMap.get(newPixel), value));
                break;
              case AVG:
              case SUM:
                pixelIntegerMap.put(newPixel, pixelIntegerMap.get(newPixel) + value);
                break;
              case COUNT:
                pixelIntegerMap.put(newPixel, 1.0);
                break;
              default:
                throw new IllegalArgumentException("Illegal PyramidTileAggre Type");
            }
          }
        }
      }
      return pixelIntegerMap;
    }

    @Override
    public TileResult<V> getResult(Map<Pixel, Double> pixelIntegerMap) {
      TileResult<V> ret = new TileResult<>();
      ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
      Double finalvalue;
      for (Map.Entry<Pixel, Double> entry : pixelIntegerMap.entrySet()) {
        if (pyramidTileAggregateType == PyramidTileAggregateType.AVG) {
          finalvalue = entry.getValue() / 4;
        } else {
          finalvalue = entry.getValue();
        }
        ret.addPixelResult(new PixelResult<>(entry.getKey(), (V) finalvalue));
      }
      return ret;
    }

    @Override
    public Map<Pixel, Double> merge(Map<Pixel, Double> acc0, Map<Pixel, Double> acc1) {
      Map<Pixel, Double> acc2 = new HashMap<>(acc0);
      acc1.forEach((key, value) -> acc2.merge(key, value, Double::sum));
      return acc2;
    }
  }
}
