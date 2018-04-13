package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class Util {
	
	public static void Print(Object o) {
        System.out.println(o);
    }
	
	/**
	 * Decide whether a location is located within a rectangle
	 * @param lat
	 * @param lon
	 * @param rect
	 * @return
	 */
	public static boolean Location_In_Rect(double lon, double lat, MyRectangle rect) {
        if (lat < rect.min_y || lat > rect.max_y || lon < rect.min_x || lon > rect.max_x) {
            return false;
        }
        return true;
    }
	
	/**
	 * Read graph from a file.
	 * @param graph_path
	 * @return
	 */
    public static ArrayList<ArrayList<Integer>> ReadGraph(String graph_path) {
        ArrayList<ArrayList<Integer>> graph = null;
        BufferedReader reader = null;
        String str = "";
        try {
            reader = new BufferedReader(new FileReader(new File(graph_path)));
            str = reader.readLine();
            int node_count = Integer.parseInt(str);
            graph = new ArrayList<ArrayList<Integer>>(node_count);
            while ((str = reader.readLine()) != null) {
                String[] l_str = str.split(",");
//                int id = Integer.parseInt(l_str[0]);
                int neighbor_count = Integer.parseInt(l_str[1]);
                ArrayList<Integer> line = new ArrayList<Integer>(neighbor_count);
                if (neighbor_count == 0) {
                    graph.add(line);
                    continue;
                }
                int i = 2;
                while (i < l_str.length) {
                    line.add(Integer.parseInt(l_str[i]));
                    ++i;
                }
                graph.add(line);
            }
        }
        catch (Exception e) {
        	Print(str);
            e.printStackTrace();
        }
        return graph;
    }
}
