package construction;

import java.util.ArrayList;
import commons.EnumVariables.UpdateStatus;
import commons.Util;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) {

    UpdateStatus status = UpdateStatus.NotUpdateInside;

    test(status);

    Util.println(status);
    // for (double x = 0.00001; x < 0.2; x *= 10)
    // {
    // Util.Print(x);
    // DecimalFormat df = new DecimalFormat("0E0");
    // String string = df.format(x);
    // Util.Print(string);
    // }

    // String teString = "Point(-5.84 54.590933333333)";
    // Util.Print(args[0]);
    // String teString = args[1];
    // String[] strings = teString.split("3\\)");
    // Util.Print(strings.length);
    // for (String string : strings)
    // Util.Print(string);

    // String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data\\wikidata";
    // String indexWholePath = dir + "\\128_128_2_whole.txt";
    //// ArrayList<VertexGeoReachList> index = Util.readGeoReachWhole(indexWholePath);
    //
    // ArrayList<ArrayList<ArrayList<Integer>>> reachgridsList = null;
    // ArrayList<ArrayList<MyRectangle>> rmbrsList = null;
    // Util.readGeoReachWhole(indexWholePath, reachgridsList, rmbrsList);
    //
    // try {
    // System.in.read();
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }

    // String graph_pos_map_path =
    // "D:\\Ubuntu_shared\\GeoMinHop\\data\\wikidata_2\\node_map_Rtree.txt";
    // Util.print("read map");
    // HashMap<String, String> graph_pos_map = Util.ReadMap(graph_pos_map_path);
    // Util.print("finish reading");
    //
    // try {
    // System.in.read();
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //
    // Util.print("ini array");
    // long[] graph_pos_map_list = new long[graph_pos_map.size()];
    // Util.print("finish ini");
    // for ( String key_str : graph_pos_map.keySet())
    // {
    // int key = Integer.parseInt(key_str);
    // int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
    // graph_pos_map_list[key] = pos_id;
    // }
    //
    // try {
    // System.in.read();
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }

    // String dir = "D:\\Ubuntu_shared\\GeoReachHop\\data\\Gowalla_10";
    // String file1 = dir + "\\128_128_3_whole.txt";
    // String file2 = dir + "\\128_128_3_whole_test.txt";
    // Util.Print(Util.compareFile(file1, file2));
  }

  public static void test(UpdateStatus status) {
    status = UpdateStatus.UpdateOutside;
    Util.println(status);
  }

  public static void test(ArrayList<Integer> arrayList) {
    // arrayList = new ArrayList<>();
    arrayList.add(0);
  }
}
