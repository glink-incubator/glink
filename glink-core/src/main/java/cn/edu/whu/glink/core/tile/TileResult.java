package cn.edu.whu.glink.core.tile;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class TileResult<V> {
  private Tile tile;
  private List<PixelResult<V>> resultList;
  private Timestamp timeStart;
  private Timestamp timeEnd;
  private Object data;

  public TileResult() {
    resultList = new ArrayList<>();
  }

  public TileResult(Tile tile) {
    this.tile = tile;
    resultList = new ArrayList<>();
  }

  public Tile getTile() {
    return tile;
  }

  public void setTile(Tile tile) {
    this.tile = tile;
  }

  public List<PixelResult<V>> getGridResultList() {
    return resultList;
  }

  public void setGridResultList(List<PixelResult<V>> result) {
    this.resultList = result;
  }

  public void addPixelResult(PixelResult<V> pixelResult) {
    resultList.add(pixelResult);
  }

  public Timestamp getTimeStart() {
    return timeStart;
  }

  public void setTimeStart(Timestamp timeStart) {
    this.timeStart = timeStart;
  }

  public Timestamp getTimeEnd() {
    return timeEnd;
  }

  public void setTimeEnd(Timestamp timeEnd) {
    this.timeEnd = timeEnd;
  }

  public void setData(Object data) {
    this.data = data;
  }

  public Object getData() {
    return data;
  }

  @Override
  public String toString() {
    StringBuilder pixels = new StringBuilder();
    for (PixelResult<V> pixelResult : resultList) {
      pixels.append("\"").append(pixelResult.getPixel().getPixelNo()).append("\"")
              .append(":")
              .append(pixelResult.getResult())
              .append(",");
    }
    pixels.setLength(pixels.length() - 1);
    return String.format("{\"level\": %d, \"x\": %d, \"y\": %d, \"start\": %tc, \"end\": %tc, \"data\": {%s}}",
            getTile().getLevel(), getTile().getX(), getTile().getY(), timeStart, timeEnd, pixels);
  }

  public boolean equals(TileResult<V> compare) {
    boolean a = compare.getTile().toLong() == this.getTile().toLong();
    boolean b = true;
    Iterator<PixelResult<V>> list1 = this.getGridResultList().iterator();
    Iterator<PixelResult<V>> list2 = compare.getGridResultList().iterator();
    while (list1.hasNext()) {
      if (!list2.hasNext()) {
        b = false;
        break;
      }
      String val1 = list1.next().getResult().toString();
      String val2 = list2.next().getResult().toString();
      if (!val1.equals(val2)) {
        b = false;
        break;
      }
    }
    if (list2.hasNext()) {
      b = false;
    }
    return a && b;
  }
}
