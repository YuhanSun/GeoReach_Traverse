package experiment;

import java.util.ArrayList;
import java.util.HashSet;

import commons.Config;
import commons.EnumVariables.*;
import commons.Util;

public class Prepare {

	static String[] datasets = new String[] {
			Datasets.foursquare.toString(), 
			Datasets.Gowalla_10.toString(), 
			Datasets.Yelp.toString()
			};
	
	static Config config = new Config();
	static String projectDir = config.getProjectDir();
	static String dataDir = config.getDataDir();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		generateStartID();
	}

	public static void generateStartID()
	{
//		for (String dataset : datasets)
		String dataset = "Patents_2_random_80";
		{
			String outputPath = String.format("%s\\query\\%s\\startID.txt", 
					projectDir, dataset);
			String graphPath = String.format("%s\\%s\\graph.txt", 
					dataDir, dataset);
			
			Util.Print("Get graph node count from " + graphPath);
			int nodeCount = Util.GetNodeCountGeneral(graphPath);
			HashSet<Long> idSet = Util.GenerateRandomInteger(nodeCount, 10000);
			ArrayList<String> idStrings = new ArrayList<>(idSet.size());
			for (Long id : idSet)
				idStrings.add(String.valueOf(id));
			Util.Print("Output to " + outputPath);
			Util.WriteArray(outputPath, idStrings);
		}
		
	}
}
