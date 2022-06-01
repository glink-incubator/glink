package cn.edu.whu.glink.core.tile;

import java.util.Objects;

/**
 * @author Yu Liebing
 */
public class Pixel {
  private final Tile tile;
  private final int pixelNo;
  private final int pixelX;
  private final int pixelY;

  public Pixel(Tile tile, int pixelX, int pixelY, int pixelNo) {
    this.tile = tile;
    this.pixelNo = pixelNo;
    this.pixelX = pixelX;
    this.pixelY = pixelY;
  }

  public int getPixelX() {
    return pixelX;
  }

  public int getPixelY() {
    return pixelY;
  }

  public Tile getTile() {
    return tile;
  }

  public int getPixelNo() {
    return pixelNo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Pixel pixel = (Pixel) o;
    return pixelNo == pixel.pixelNo && Objects.equals(tile, pixel.tile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tile, pixelX, pixelY);
  }

  @Override
  public String toString() {
    return "Pixel{"
            + "tile=" + tile
            + ", pixelNo=" + pixelNo
            + ", pixelX=" + pixelX
            + ", pixelY=" + pixelY
            + '}';
  }
}
