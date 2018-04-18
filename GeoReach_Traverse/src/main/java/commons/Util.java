package commons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.roaringbitmap.RoaringBitmap;

public class Util {
	
	public static void Print(Object o) {
        System.out.println(o);
    }
	
	public static int GetSpatialEntityCount(ArrayList<Entity> entities)
    {
    	int count = 0;
    	for ( Entity entity : entities)
    		if(entity.IsSpatial)
    			count++;
    	return count;
    }
	
	public static void WriteArray(String filename, ArrayList<String> arrayList)
    {
    	FileWriter fileWriter = null;
    	try {
			fileWriter = new FileWriter(new File(filename));
			for (String line : arrayList)
				fileWriter.write(line + "\n");
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
	/**
	 * Generate a set of random values for a given range.
	 * @param graph_size [0, graph_size - 1]
	 * @param node_count the wanted set size
	 * @return a set of values
	 */
	public static HashSet<Long> GenerateRandomInteger(long graph_size, int node_count) {
        HashSet<Long> ids = new HashSet<Long>();
        Random random = new Random();
        while (ids.size() < node_count) {
            Long id = (long)(random.nextDouble() * (double)graph_size);
            ids.add(id);
        }
        return ids;
    }
	
	/**
     * Get node count from a graph file.
     * The graph has to be the correct format.
     * @param filepath
     * @return
     */
    public static int GetNodeCountGeneral(String filepath) {
        int node_count = 0;
        File file = null;
        BufferedReader reader = null;
        try {
            try {
                file = new File(filepath);
                reader = new BufferedReader(new FileReader(file));
                String str = reader.readLine();
                String[] l = str.split(" ");
                node_count = Integer.parseInt(l[0]);
            }
            catch (Exception e) {
                e.printStackTrace();
                try {
                    reader.close();
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        finally {
            try {
                reader.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return node_count;
    }
	
	/**
	 * Read map from file
	 * @param filename
	 * @return
	 */
	public static HashMap<String, String> ReadMap(String filename)
	{
		try {
			HashMap<String, String> map = new HashMap<String, String>();
			BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
			String line = null;
			while ( (line = reader.readLine()) != null)
			{
				String[] liStrings = line.split(",");
				map.put(liStrings[0], liStrings[1]);
			}
			reader.close();
			return map;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		Util.Print("nothing in ReadMap(" + filename + ")");
		return null;
	}
	
	public static String Serialize_RoarBitmap_ToString(RoaringBitmap r) {
        r.runOptimize();
        ByteBuffer outbb = ByteBuffer.allocate(r.serializedSizeInBytes());
        try {
        	r.serialize(new DataOutputStream(new OutputStream(){
			    ByteBuffer mBB;
			    OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
			    public void close() {}
			    public void flush() {}
			    public void write(int b) {
			        mBB.put((byte) b);}
			    public void write(byte[] b) {mBB.put(b);}            
			    public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
			}.init(outbb)));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        outbb.flip();
        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
        return serializedstring;
    }
	
	public static HashMap<Integer, Integer> histogram(List<Integer> list)
	{
		HashMap<Integer, Integer> res = new HashMap<>();
		for (int element : list)
		{
			if (res.containsKey(element))
				res.put(element, res.get(element) + 1);
			else
				res.put(element, 1);
		}
		return res;
	}
	
	/**
	 * Decide whether a location is located within a rectangle
	 * @param lon
	 * @param lat
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
    
    /**
     * Output everything.
     * Each vertex for each step contains all the ReachGrid and RMBR information
     * @param index
     * @param filepath
     */
    public static void outputGeoReach(ArrayList<VertexGeoReach> index, String filepath)
    {
    	int id = 0;
    	FileWriter writer = null;
    	try
    	{
    		int nodeCount = index.size();
    		int MAX_HOP = index.get(0).ReachGrids.size();
    		writer = new FileWriter(filepath);
    		writer.write(String.format("%d,%d\n", nodeCount, MAX_HOP));
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
    
    /**
     * Output index based on type
     * @param index
     * @param filepath
     * @param types
     * @param format 0 represents list while 1 represents bitmap
     */
    public static void outputGeoReach(ArrayList<VertexGeoReach> index, String filepath, 
    		ArrayList<ArrayList<Integer>> typesList, int format)
    {
    	int id = 0;
    	FileWriter writer = null;
    	try
    	{
    		int nodeCount = index.size();
    		int MAX_HOP = index.get(0).ReachGrids.size();
    		writer = new FileWriter(filepath);
    		writer.write(String.format("%d,%d\n", nodeCount, MAX_HOP));
    		
    		Iterator<VertexGeoReach> iterator1 = index.iterator();
    		Iterator<ArrayList<Integer>> iterator2 = typesList.iterator();
    		
    		while (iterator1.hasNext() && iterator2.hasNext())
        	{
    			VertexGeoReach vertexGeoReach = iterator1.next();
    			ArrayList<Integer> types = iterator2.next();
    			
    			writer.write(id + "\n");
    			for ( int i = 0; i < MAX_HOP; i++)
    			{
    				int type = types.get(i);
    				writer.write("" + type + ":");
    				switch (type) {
					case 0:
						TreeSet<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
						if (format == 1)
						{
							RoaringBitmap r = new RoaringBitmap();
							for(int gridID : reachgrid)
								r.add(gridID);
							String bitmap_ser = Util.Serialize_RoarBitmap_ToString(r);
							writer.write(bitmap_ser + "\n");
						}
						else {
							writer.write(reachgrid.toString() + "\n");
						}
						break;
					case 1:
						writer.write(vertexGeoReach.RMBRs.get(i).toString() + "\n");
						break;
					case 2:
						writer.write(vertexGeoReach.GeoBs.get(i).toString() + "\n");
						break;
					default:
						throw new Exception(String.format("Wrong type %d for vertex %d!", 
								type, id));
					}
    			}
        		id++;
        	}
    		writer.close();
    	}
    	catch(Exception e)
    	{
    		Util.Print(id);
    		Util.Print(typesList.get(id));
    		e.printStackTrace();
    		System.exit(-1);
    	}
    }
}
