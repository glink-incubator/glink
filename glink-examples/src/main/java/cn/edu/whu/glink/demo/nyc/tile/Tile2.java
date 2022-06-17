package cn.edu.whu.glink.demo.nyc.tile;

/**
 * @author Xu Qi
 */
public class Tile2 {
  private String tileID;
  private Object userData = null;

  public Tile2() {
  }

  public Tile2(String tileID) {
    this.tileID = tileID;
  }

  public String getTileID() {
    return tileID;
  }

  public void setTileID(String tileID) {
    this.tileID = tileID;
  }

  public Object getUserData() {
    return userData;
  }

  public void setUserData(Object userData) {
    this.userData = userData;
  }
}
