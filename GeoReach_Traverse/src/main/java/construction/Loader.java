package construction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import commons.Config;
import commons.Entity;
import commons.Util;
import commons.Config.system;

public class Loader {
	Config config;
	String dataset, version;
	system systemName;
	int MAX_HOPNUM;
	double minx, miny, maxx, maxy;
	
	ArrayList<ArrayList<Integer>> graph;
	ArrayList<Entity> entities;
	String dbPath, entityPath, graph_pos_map_path, graphPath;
	
	String reachGridName, rmbrName, geoBName;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public void load(String indexPath, String dbPath, long[] graph_pos_map_list)
	{
		int lineIndex = 0;
		String line = "";
		try {
			Map<String, String> config = new HashMap<String, String>();
			config.put("dbms.pagecache.memory", "20g");
			BatchInserter inserter = BatchInserters.inserter(
					new File(dbPath).getAbsoluteFile(), config);
			
			BufferedReader reader = new BufferedReader(
					new FileReader(new File(indexPath)));
			
			lineIndex++;
			line = reader.readLine();
			String[] strList = line.split(",");
			int nodeCount = Integer.parseInt(strList[0]);
			int MAX_HOP = Integer.parseInt(strList[1]);
			
			for ( int i = 0; i < nodeCount; i++)
			{
				lineIndex++;
				int id = Integer.parseInt(reader.readLine());
				long neo4j_ID = graph_pos_map_list[id];
				for ( int j = 0; j < MAX_HOP; j++)
				{
					lineIndex++;
					line = reader.readLine();
					strList = line.split(":");
					int type = Integer.parseInt(strList[0]);
					switch (type) {
					case 0:
						inserter.setNodeProperty(neo4j_ID, reachGridName+"_"+j, strList[1]);
						break;
					case 1:
						inserter.setNodeProperty(neo4j_ID, rmbrName+"_"+j, strList[1]);
						break;
					case 2:
						if (strList[1].equals("true"))
							inserter.setNodeProperty(neo4j_ID, geoBName+"_"+j, true);
						else {
							inserter.setNodeProperty(neo4j_ID, geoBName+"_"+j, false);
						}
					default:
						throw new Exception(String.format("Vertex %d hop %d has type %d!", 
								id, j, type));
					}
				}
			}
			
			inserter.shutdown();
			
		} catch (Exception e) {
			Util.Print(String.format("line %d: %s", lineIndex, line));
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	
	public Loader(Config config)
	{
		this.config = config;
		initParameters();
	}
	
	public void initParameters()
	{
		systemName = config.getSystemName();
		version = config.GetNeo4jVersion();
		dataset = config.getDatasetName();
		MAX_HOPNUM = config.getMaxHopNum();
		reachGridName = config.getReachGridName();
		rmbrName = config.getRMBRName();
		geoBName = config.getGeoBName();
		switch (systemName) {
		case Ubuntu:
			dbPath = String.format("/home/yuhansun/Documents/GeoReachHop/%s_%s/data/databases/graph.db", version, dataset);
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			break;
		case Windows:
			dbPath = String.format("D:\\Ubuntu_shared\\GeoReachHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", 
					dataset, version, dataset);
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
		default:
			break;
		}
		if (dataset.contains("Gowalla") || dataset.contains("Yelp")
				|| dataset.contains("foursquare"))
		{
			minx = -180;
			miny = -90;
			maxx = 180;
			maxy = 90; 
		}
		if (dataset.contains("Patents") || dataset.contains("go_uniprot"))
		{
			minx = 0;
			miny = 0;
			maxx = 1000;
			maxy = 1000;
		}
	}

}
