package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
            System.exit(-1);
        }
        return graph;
    }
    
    /**
     * Read entities
     * @param entity_path
     * @return
     */
    public static ArrayList<Entity> ReadEntity(String entity_path) {
        ArrayList<Entity> entities = null;
        BufferedReader reader = null;
        String str = null;
        int id = 0;
        try {
            reader = new BufferedReader(new FileReader(new File(entity_path)));
            str = reader.readLine();
            int node_count = Integer.parseInt(str);
            entities = new ArrayList<Entity>(node_count);
            while ((str = reader.readLine()) != null) {
                Entity entity;
                String[] str_l = str.split(",");
                int flag = Integer.parseInt(str_l[1]);
                if (flag == 0) {
                    entity = new Entity(id);
                    entities.add(entity);
                } else {
                    entity = new Entity(id, Double.parseDouble(str_l[2]), Double.parseDouble(str_l[3]));
                    entities.add(entity);
                }
                ++id;
            }
            reader.close();
        }
        catch (Exception e) {
            Util.Print(String.format("error happens in entity id %d", id));
            e.printStackTrace();	System.exit(-1);
        }
        return entities;
    }
    
    public static void outputGeoReach(ArrayList<VertexGeoReach> index, String filepath)
    {
    	int id = 0;
    	FileWriter writer = null;
    	try
    	{
    		int nodeCount = index.size();
    		int MAX_HOP = index.get(0).ReachGrids.size();
    		writer = new FileWriter(filepath);
    		writer.write(String.format("%d,%d", nodeCount, MAX_HOP));
    		for (VertexGeoReach vertexGeoReach : index)
        	{
        		writer.write(id + "\n");
        		writer.write(vertexGeoReach.toString());
        		id++;
        	}
    		writer.close();
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    		System.exit(-1);
    	}
    	
    }
}
