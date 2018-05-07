package commons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.roaringbitmap.RoaringBitmap;

public class Util {
	
	public static void Print(Object o) {
        System.out.println(o);
    }
	
	public static GraphDatabaseService getDatabaseService(String dbPath)
	{
		if (!Util.pathExist(dbPath))
		{
			Util.Print(dbPath + "does not exist!");
			System.exit(-1);
		}
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
		return dbservice;
	}
	
	
	public static long Average(ArrayList<Long> arraylist)
	{
		if ( arraylist .size() == 0)
			return -1;
		long sum = 0;
		for ( long element : arraylist)
			sum += element;
		return sum / arraylist.size();
	}
	
	/**
	 * This function has to be used in transaction.
	 * @param databaseService
	 * @param ids
	 * @return
	 */
	public static ArrayList<Node> getNodesByIDs(GraphDatabaseService databaseService, ArrayList<Long> ids) 
	{
		ArrayList<Node> nodes = new ArrayList<>(); 
		for ( long id : ids)
			nodes.add(databaseService.getNodeById(id));
		return nodes;
	}
	
	public static String clearAndSleep(String password, int sleepTime)
	{
		String res = clearCache(password);
		Thread.currentThread();
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public static String clearCache(String password) {
        String[] cmd = new String[]{"/bin/bash", "-c", "echo " + password + " | sudo -S sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""};
        String result = null;
        try {
            String line;
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();
            result = String.valueOf(result) + "\n";
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
	
	public static ArrayList<MyRectangle> ReadQueryRectangle(String filepath) {
		ArrayList<MyRectangle> queryrectangles;
		queryrectangles = new ArrayList<MyRectangle>();
		BufferedReader reader = null;
		File file = null;
			try {
				file = new File(filepath);
				reader = new BufferedReader(new FileReader(file));
				String temp = null;
				while ((temp = reader.readLine()) != null) {
					if ( temp.contains("%"))
						continue;
					String[] line_list = temp.split("\t");
					MyRectangle rect = new MyRectangle(Double.parseDouble(line_list[0]), Double.parseDouble(line_list[1]), Double.parseDouble(line_list[2]), Double.parseDouble(line_list[3]));
					queryrectangles.add(rect);
				}
				reader.close();
			}
			catch (Exception e) {
				e.printStackTrace();
		}
		finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return queryrectangles;
	}
	
	public static void WriteFile(String filename, boolean app, String str) {
        try {
            FileWriter fw = new FileWriter(filename, app);
            fw.write(str);
            fw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	/**
	 * read integer arraylist
	 * @param path
	 * @return
	 */
	public static ArrayList<Integer> readIntegerArray(String path)
	{
		String line = null;
		ArrayList<Integer> arrayList = new ArrayList<Integer>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
			while ( (line = reader.readLine()) != null )
			{
				int x = Integer.parseInt(line);
				arrayList.add(x);
			}
			reader.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return arrayList;
	}
	
	/**
	 * Get db hits. 
	 * Directly use getDBHits will just get the hits of the final step.
	 * It is not feasible.
	 * @param plan
	 * @return
	 */
	public static long GetTotalDBHits(ExecutionPlanDescription plan)
    {
    	long dbhits = 0;
    	Queue<ExecutionPlanDescription> queue = new LinkedList<ExecutionPlanDescription>();
    	if(plan.hasProfilerStatistics())
    		queue.add(plan);
    	while(queue.isEmpty() == false)
    	{
    		ExecutionPlanDescription planDescription = queue.poll();
    		dbhits += planDescription.getProfilerStatistics().getDbHits();
    		for ( ExecutionPlanDescription planDescription2 : planDescription.getChildren())
    			queue.add(planDescription2);
    	}
    	return dbhits;
    }
	
	public static void WriteFile(String filename, boolean app, List<String> lines) {
        try {
            FileWriter fw = new FileWriter(filename, app);
            int i = 0;
            while (i < lines.size()) {
                fw.write(String.valueOf(lines.get(i)) + "\n");
                ++i;
            }
            fw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public static void WriteFile(String filename, boolean app, Set<String> lines) {
        try {
            FileWriter fw = new FileWriter(filename, app);
            for (String line : lines)
            	fw.write(line + "\n");
            fw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public static boolean pathExist(String path)
	{
		File file = new File(path);

		if (file.exists()) 
			return true;
		else
			return false;
	}
	
	public static boolean intersect(MyRectangle rect1, MyRectangle rect2)
    {
    	if(rect1.min_x > rect2.max_x || rect1.min_y > rect2.max_y || rect1.max_x < rect2.min_x || rect1.max_y < rect2.min_y)
    		return false;
    	else
    		return true;
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
            int nodeCount = Integer.parseInt(str);
            graph = new ArrayList<ArrayList<Integer>>(nodeCount);
            int index = 0;
            while ((str = reader.readLine()) != null) {
                String[] l_str = str.split(",");
                int id = Integer.parseInt(l_str[0]);
                if (id != index)
                	throw new Exception(String.format("this line has id %d, but the index should be %d!", id, index));
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
            if (nodeCount != index + 1)
            	throw new Exception(String.format("first line shows node count is %d, but only has %d lines!", nodeCount, index));
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
    
    public static ArrayList<VertexGeoReachList> readGeoReachWhole(String filepath)
    {
    	int id = 0;
    	String line = null;
    	BufferedReader reader = null;
    	try {
			reader = new BufferedReader(new FileReader(new File(filepath)));
			line = reader.readLine();
			String[] strList = line.split(",");
			int nodeCount = Integer.parseInt(strList[0]);
			int MAX_HOPNUM = Integer.parseInt(strList[1]);
			ArrayList<VertexGeoReachList> index = new ArrayList<>(nodeCount);
			// each vertex
			for ( int i = 0; i < nodeCount; i++)
			{
				VertexGeoReachList vertexGeoReachList = new VertexGeoReachList(MAX_HOPNUM);
				id = Integer.parseInt(reader.readLine());
				
				// each hop
				for ( int j = 0; j < MAX_HOPNUM; j++)
				{
					line = reader.readLine();
					strList = line.split(";");
					
					String reachGridStr = strList[0];
					String rmbrStr = strList[1];
//					String geoBStr = strList[2];
					
					// reachGrid
					if (!reachGridStr.equals("null"))
					{
						reachGridStr = reachGridStr.substring(1, reachGridStr.length() - 1);
						strList = reachGridStr.split(", ");
						ArrayList<Integer> reachGrid = new ArrayList<>(strList.length);
						for ( String string : strList)
							reachGrid.add(Integer.parseInt(string));
						vertexGeoReachList.ReachGrids.set(j, reachGrid);
					}
					
					//rmbr
					if (!rmbrStr.equals("null"))
					{
						MyRectangle rmbr = new MyRectangle(rmbrStr);
						vertexGeoReachList.RMBRs.set(j, rmbr);
					}
				}
				index.add(vertexGeoReachList);
			}
			reader.close();
			return index;
		} catch (Exception e) {
			Util.Print("id: " + id + "\n" + line);
			e.printStackTrace();
			System.exit(-1);
		}
    	return null;
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
    		Util.Print("Error happens when output index for vertex " + id);
    		Util.Print("Type List: " + typesList.get(id));
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
    public static void outputGeoReachForList(ArrayList<VertexGeoReachList> index, String filepath, 
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
    		
    		Iterator<VertexGeoReachList> iterator1 = index.iterator();
    		Iterator<ArrayList<Integer>> iterator2 = typesList.iterator();
    		
    		while (iterator1.hasNext() && iterator2.hasNext())
        	{
    			VertexGeoReachList vertexGeoReach = iterator1.next();
    			ArrayList<Integer> types = iterator2.next();
    			
    			writer.write(id + "\n");
    			for ( int i = 0; i < MAX_HOP; i++)
    			{
    				int type = types.get(i);
    				writer.write("" + type + ":");
    				switch (type) {
					case 0:
						ArrayList<Integer> reachgrid = vertexGeoReach.ReachGrids.get(i);
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
    		Util.Print("Error happens when output index for vertex " + id);
    		Util.Print("Type List: " + typesList.get(id));
    		e.printStackTrace();
    		System.exit(-1);
    	}
    }
}
