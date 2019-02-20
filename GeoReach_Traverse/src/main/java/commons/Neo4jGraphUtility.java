package commons;

import java.io.File;
import java.util.HashSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4jGraphUtility {

  /**
   * Get the normal db directory name.
   *
   * @param piecesX
   * @param piecesY
   * @param MG
   * @param MR
   * @param MC
   * @return
   */
  public static String getDbNormalName(int piecesX, int piecesY, double MG, double MR, int MC) {
    return String.format("graph_%d_%d_%d_%d_%d.db", piecesX, piecesY, (int) MG * 100,
        (int) MR * 100, MC);
  }

  /**
   * Return all the graph neighbors for the given node. Only consider "GRAPH_LINK". RTree-related
   * edges are not included.
   * 
   * @param node
   * @return
   */
  public static HashSet<Node> getGraphNeighbors(Node node) {
    Iterable<Relationship> rels = node.getRelationships(RelationshipType.withName("GRAPH_LINK"));
    HashSet<Node> neighbors = new HashSet<>();
    for (Relationship relationship : rels) {
      neighbors.add(relationship.getOtherNode(node));
    }
    return neighbors;
  }

  /**
   * Get dababase service. Stop the program if dbPath does not exist.
   *
   * @param dbPath
   * @return
   */
  public static GraphDatabaseService getDatabaseService(String dbPath) {
    if (!Util.pathExist(dbPath)) {
      Util.println(dbPath + "does not exist!");
      System.exit(-1);
    }
    GraphDatabaseService dbservice =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
    return dbservice;
  }

  public static void printNode(Node node) {
    Util.println(String.format("%s: %s", node, node.getAllProperties()));
  }

}
