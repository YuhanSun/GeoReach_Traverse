import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import commons.EnumVariables.Expand;
import commons.EnumVariables.MaintenanceStrategy;
import commons.Util;
import dataprocess.Wikidata;
import experiment.AddEdge;

public class Driver {

  // function names
  private static enum FunctionName {
    query, insertion, generateAccurateDb, //
    wikidataExtractProperties, //
    wikidataExtractStringLabel, //
    wikidataExtractEntityToEntityRelationEdgeFormat, //
    wikidataLoadGraph,
  }

  private String[] args = null;
  private Options options = new Options();

  private String help = "h";
  private String function = "f";
  private String homeDir = "hd";
  private String dataset = "d";
  private String resultDir = "rd";

  private String MG = "MG";
  private String MR = "MR";

  private String strategy = "us";
  private String expand = "ex";
  private String testRatio = "testRatio";


  public Driver(String[] args) {
    this.args = args;
    options.addOption(help, "help", false, "show help.");
    options.addOption(function, "function", true, "function");
    // directory
    options.addOption(homeDir, "home-directory", true, "home directory");
    options.addOption(dataset, "dataset", true, "dataset for folder name");
    options.addOption(resultDir, "result-directory", true, "result directory");

    // index parameter
    options.addOption(MG, "MG", true, "MG");
    options.addOption(MR, "MR", true, "MR");

    //
    options.addOption(strategy, "update-strategy", true,
        "update strategy (lightweight or reconstruct)");
    options.addOption(expand, "expand", true, "graph expand method (simple or spatraversal");
    options.addOption(testRatio, "test-ratio", true, "add edges ratio");

  }

  public void parser() {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      Option[] options = cmd.getOptions();
      for (Option option : options) {
        Util.println(String.format("<%s, %s>", option, option.getValue()));
      }

      if (cmd.hasOption("h")) {
        help();
      }

      if (cmd.hasOption(function)) {
        String functionNameString = cmd.getOptionValue(function);
        FunctionName functionName = FunctionName.valueOf(functionNameString);
        AddEdge addEdge = new AddEdge();
        addEdge.iniPaths(cmd.getOptionValue(homeDir), cmd.getOptionValue(resultDir),
            cmd.getOptionValue(dataset));
        double MGVal = Double.parseDouble(cmd.getOptionValue(MG));
        double MRVal = Double.parseDouble(cmd.getOptionValue(MR));
        switch (functionName) {
          case insertion:
            int partCount = 4;
            addEdge.readGraphEntityAndLabelList();
            addEdge.evaluateEdgeInsersion(MGVal, MRVal,
                Double.parseDouble(cmd.getOptionValue(testRatio)), partCount,
                MaintenanceStrategy.valueOf(cmd.getOptionValue(strategy)));
            break;
          case query:
            addEdge.evaluateInsertionByQuery(MGVal, MRVal,
                MaintenanceStrategy.valueOf(cmd.getOptionValue(strategy)),
                Expand.valueOf(cmd.getOptionValue(expand)));
            break;
          case generateAccurateDb:
            addEdge.generateAccurateDbAfterAddEdges(MGVal, MRVal);
            break;
          case wikidataExtractProperties:
            Wikidata wikidata = new Wikidata(cmd.getOptionValue(homeDir));
            wikidata.extractProperties();
            break;
          case wikidataExtractStringLabel:
            wikidata = new Wikidata(cmd.getOptionValue(homeDir));
            wikidata.extractStringLabels();
            break;
          case wikidataExtractEntityToEntityRelationEdgeFormat:
            wikidata = new Wikidata(cmd.getOptionValue(homeDir));
            wikidata.extractEntityToEntityRelationEdgeFormat();
            break;
          case wikidataLoadGraph:
            wikidata = new Wikidata(cmd.getOptionValue(homeDir));
            wikidata.loadAllEntityAndCreateIdMap();
          default:
            Util.println(String.format("Function %s does not exist!", functionNameString));
            break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private void help() {
    HelpFormatter formater = new HelpFormatter();
    formater.printHelp("Main", options);
    System.exit(0);
  }


  public static void main(String[] args) {
    Util.println(Arrays.toString(args));
    Driver driver = new Driver(args);
    driver.parser();
  }

}
