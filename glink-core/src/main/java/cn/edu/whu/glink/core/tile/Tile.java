package cn.edu.whu.glink.core.tile;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Yu Liebing
 */
public class Tile implements Serializable {
  private final int level;
  private final int x;
  private final int y;

  public Tile(int level, int x, int y) {
    this.level = level;
    this.x = x;
    this.y = y;
  }

  public int getLevel() {
    return level;
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Tile tile = (Tile) o;
    return level == tile.level && x == tile.x && y == tile.y;
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, x, y);
  }

  @Override
  public String toString() {
    return String.format("Tile{level=%d, x=%d, y=%d}", level, x, y);
  }

  /**
   * Get id of the tile.
   */
  public long toLong() {
    long res = 0L;
    res |= ((long) level) << 38;
    for (int i = 0; i < 38; i++) {
      // even for x, odd for y
      int ind = i / 2;
      if (i % 2 == 0) {
        res |= ((long) (x >>> ind & 1) << i);
      } else {
        res |= ((long) (y >>> ind & 1) << i);
      }
    }
    return res;
  }

  public Tile fromLong(long tileId) {
    int level = (int) (tileId >> 38);
    long x = 0L;
    long y = 0L;
    for (int i = 0; i < 38; i++) {
      // even for x, odd for y
      int ind = i / 2;
      if (i % 2 == 0) {
        x |= (tileId >> i & 1) << ind;
      } else {
        y |= (tileId >> i & 1) << ind;
      }
    }
    return new Tile(level, (int) x, (int) y);
  }

  public Tile getUpperTile() {
    int newX = (int) ((double) x / 2);
    int newY = (int) ((double) y / 2);
    int newLevel = level - 1;
    return new Tile(newLevel, newX, newY);
  }
}
