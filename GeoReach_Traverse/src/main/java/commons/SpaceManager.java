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
  private double minx, miny, maxx, maxy;

  /**
   * The number of cells on each dimension.
   */
  private int piecesX, piecesY;

  private double totalArea;

  /**
   * The width and height of each cell.
   */
  private double resolutionX, resolutionY;

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
    totalArea = (maxx - minx) * (maxy - miny);
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
    totalArea = rectangle.area();
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

  public double getTotalArea() {
    return totalArea;
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
   * Get the [idX_min, idY_min, idX_max, idY_max, count] as the 2-D boundary for the given
   * reachgrid.
   *
   * @param immutableRoaringBitmap
   * @return the boundary in [0, 3]. [4] stores the # of elements in the iterable
   */
  public int[] getXYBoundary(Iterable<Integer> iterable) {
    int[] boundary =
        new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, 0};
    int count = 0;
    for (int id : iterable) {
      count++;
      int[] idXY = getXYId(id);
      boundary[0] = Math.min(idXY[0], boundary[0]);
      boundary[1] = Math.min(idXY[1], boundary[1]);
      boundary[2] = Math.max(idXY[0], boundary[2]);
      boundary[3] = Math.max(idXY[1], boundary[3]);
    }
    boundary[4] = count;
    return boundary;
  }

  /**
   * Get all the grid ids covered by a given rectangle.
   *
   * @param rectangle
   * @return
   */
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

  /**
   * Get the Mbr of a given cell [idX, idY].
   *
   * @param idX
   * @param idY
   * @return
   */
  public MyRectangle getMbrOfCell(int idX, int idY) {
    double minx = resolutionX * idX;
    double miny = resolutionY * idY;
    double maxx = resolutionX * (idX + 1);
    double maxy = resolutionY * (idY + 1);
    return new MyRectangle(minx, miny, maxx, maxy);
  }

  /**
   * Get the Mbr of a given ReachGrid.
   *
   * @param immutableRoaringBitmap
   * @return
   */
  public MyRectangle getMbrOfReachGrid(ImmutableRoaringBitmap immutableRoaringBitmap) {
    int[] xyBoundary = getXYBoundary(immutableRoaringBitmap);
    return getMbrOfBoundary(xyBoundary);
  }

  /**
   * Get the Mbr of a given boundary.
   *
   * @param boundary
   * @return
   */
  public MyRectangle getMbrOfBoundary(int[] xyBoundary) {
    MyRectangle leftBottom = getMbrOfCell(xyBoundary[0], xyBoundary[1]);
    MyRectangle rightTop = getMbrOfCell(xyBoundary[2], xyBoundary[3]);
    return new MyRectangle(leftBottom.min_x, leftBottom.min_y, rightTop.max_x, rightTop.max_y);
  }
}
