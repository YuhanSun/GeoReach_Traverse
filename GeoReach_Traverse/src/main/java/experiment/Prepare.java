package experiment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import commons.Config;
import commons.EnumVariables.*;
import commons.Util;

public class Prepare {

	static String[] datasets = new String[] {
//			Datasets.foursquare.name(), 
//			Datasets.Gowalla_10.name(), 
//			Datasets.Yelp.name(),
			Datasets.Patents_2_random_80.name(),
			Datasets.wikidata_2.name(),
			};
	
	static Config config = new Config();
	static String projectDir = config.getProjectDir();
	static String dataDir = config.getDataDir();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		generateStartID();
		convertStartID();
	}

	/**
	 * Convert id to the neo4j node id.
	 */
	public static void convertStartID()
	{
		for (String dataset : datasets)
		{
			String stardIDPath = String.format("%s\\query\\%s\\startID.txt", 
					projectDir, dataset);
			ArrayList<Integer> startIDs = Util.readIntegerArray(stardIDPath);
			
			String graph_pos_map_path = String.format("%s\\%s\\node_map_RTree.txt", dataDir, dataset);
	    	Util.print("read map from " + graph_pos_map_path);
	    	HashMap<String, String> graph_pos_map = Util.ReadMap(graph_pos_map_path);
	    	Util.print("finish reading");
	    	
	    	Util.print("ini array");
			long[] graph_pos_map_list = new long[graph_pos_map.size()];
			Util.print("finish ini");
			for ( String key_str : graph_pos_map.keySet())
			{
				int key = Integer.parseInt(key_str);
				int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
				graph_pos_map_list[key] = pos_id;
			}
			
			ArrayList<String> startIDNeo4j = new ArrayList<>(startIDs.size());
			for (int id : startIDs)
				startIDNeo4j.add(String.valueOf(graph_pos_map_list[id]));
			String outputPath = String.format("%s\\query\\%s\\startID_neo4j.txt", 
					projectDir, dataset);
			Util.print("output neo4j ids to " + outputPath);
			Util.WriteArray(outputPath, startIDNeo4j);
		}
	}
	
	/**
	 * Randomly generate id between [0, graphsize-1]
	 */
	public static void generateStartID()
	{
//		for (String dataset : datasets)
		String dataset = "wikidata";
		{
			String outputPath = String.format("%s\\query\\%s\\startID.txt", 
					projectDir, dataset);
			String graphPath = String.format("%s\\%s\\graph.txt", 
					dataDir, dataset);
			
			Util.print("Get graph node count from " + graphPath);
			int nodeCount = Util.GetNodeCountGeneral(graphPath);
			HashSet<Long> idSet = Util.GenerateRandomInteger(nodeCount, 10000);
			ArrayList<String> idStrings = new ArrayList<>(idSet.size());
			for (Long id : idSet)
				idStrings.add(String.valueOf(id));
			Util.print("Output to " + outputPath);
			Util.WriteArray(outputPath, idStrings);
		}
	}
}
