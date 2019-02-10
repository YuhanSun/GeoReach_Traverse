package commons;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * This class manages a space and provides utilities for converting from a location to an id.
 *
 * @author Yuhan Sun
 *
 */
public class SpaceManager {
  /**
   * The boundary of the space.
   */
  public double minx, miny, maxx, maxy;

  /**
   * The number of cells on each dimension.
   */
  public int piecesX, piecesY;

  /**
   * The width and height of each cell.
   */
  public double resolutionX, resolutionY;

  public SpaceManager(double minx, double miny, double maxx, double maxy, int piecesX,
      int piecesY) {
    this.minx = minx;
    this.miny = miny;
    this.maxx = maxx;
    this.maxy = maxy;
    this.piecesX = piecesX;
    this.piecesY = piecesY;
    resolutionX = (maxx - minx) / (double) piecesX;
    resolutionY = (maxy - miny) / (double) piecesY;
  }

  public SpaceManager(MyRectangle rectangle, int piecesX, int piecesY) {
    this.minx = rectangle.min_x;
    this.miny = rectangle.min_y;
    this.maxx = rectangle.max_x;
    this.maxy = rectangle.max_y;
    this.piecesX = piecesX;
    this.piecesY = piecesY;
    resolutionX = (maxx - minx) / (double) piecesX;
    resolutionY = (maxy - miny) / (double) piecesY;
  }

  public double getMaxx() {
    return maxx;
  }

  public void setMaxx(double maxx) {
    this.maxx = maxx;
  }

  public double getMaxy() {
    return maxy;
  }

  public void setMaxy(double maxy) {
    this.maxy = maxy;
  }

  public int getPiecesX() {
    return piecesX;
  }

  public void setPiecesX(int pieces_x) {
    this.piecesX = pieces_x;
  }

  public int getPiecesY() {
    return piecesY;
  }

  public void setPiecesY(int pieces_y) {
    this.piecesY = pieces_y;
  }

  public double getResolutionX() {
    return resolutionX;
  }

  public double getResolutionY() {
    return resolutionY;
  }

  public double getMiny() {
    return miny;
  }

  public void setMiny(double miny) {
    this.miny = miny;
  }

  public double getMinx() {
    return minx;
  }

  public void setMinx(double minx) {
    this.minx = minx;
  }

  /**
   * Get the [idX, idY] of a given location (x, y).
   *
   * @param x
   * @param y
   * @return
   */
  public int[] getXYId(double x, double y) {
    int idX = Math.min(piecesX - 1, (int) ((x - minx) / resolutionX));
    int idY = Math.min(piecesY - 1, (int) ((y - miny) / resolutionY));
    return new int[] {idX, idY};
  }

  /**
   * Get the [idX, idY] of a given cell id.
   *
   * @param id
   * @return [idX, idY]
   */
  public int[] getXYId(int id) {
    int idX = id / piecesX;
    int idY = id - piecesX * idX;
    int[] xy = new int[] {idX, idY};
    return xy;
  }

  /**
   * Get the unique id for a [idX, idY].
   *
   * @param idX
   * @param idY
   * @return
   */
  public int getId(int idX, int idY) {
    return idX * piecesY + idY;
  }

  /**
   * Get the unique id for a location [x, y].
   *
   * @param x
   * @param y
   * @return
   */
  public int getId(double x, double y) {
    int[] idXY = getXYId(x, y);
    return getId(idXY[0], idXY[1]);
  }

  /**
   * Get the [idX_min, idY_min, idX_max, idY_max] boundary of a rectangle.
   *
   * @param rmbr
   * @return [idX_min, idY_min, idY_max, idY_max]
   */
  public int[] getXYBoundary(MyRectangle rmbr) {
    int[] xyLB = getXYId(rmbr.min_x, rmbr.min_y);
    int[] xyRT = getXYId(rmbr.max_x, rmbr.max_y);
    return new int[] {xyLB[0], xyLB[1], xyRT[0], xyRT[1]};
  }

  /**
   * Get the [idX_min, idY_min, idX_max, idY_max] as the 2-D boundary for the given reachgrid.
   *
   * @param immutableRoaringBitmap
   * @return
   */
  public int[] getXYBoundary(ImmutableRoaringBitmap immutableRoaringBitmap) {
    int[] boundary =
        new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
    for (int id : immutableRoaringBitmap) {
      int[] idXY = getXYId(id);
      boundary[0] = Math.min(idXY[0], boundary[0]);
      boundary[1] = Math.min(idXY[1], boundary[1]);
      boundary[2] = Math.max(idXY[0], boundary[2]);
      boundary[3] = Math.max(idXY[1], boundary[3]);
    }
    return boundary;
  }

  public ImmutableRoaringBitmap getCoverIdOfRectangle(MyRectangle rectangle) {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    int[] xyBoundary = getXYBoundary(rectangle);
    for (int i = xyBoundary[0]; i <= xyBoundary[2]; i++) {
      for (int j = xyBoundary[1]; j <= xyBoundary[3]; j++) {
        int grid_id = i * piecesY + j;
        roaringBitmap.add(grid_id);
      }
    }
    return roaringBitmap.toMutableRoaringBitmap().toImmutableRoaringBitmap();
  }
}
