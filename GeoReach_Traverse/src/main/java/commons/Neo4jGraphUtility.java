package commons;

import java.io.File;
import java.util.HashSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class Neo4jGraphUtility {

  /**
   * Assume that the property exists. Otherwise throw exception.
   *
   * @param node
   * @param property
   * @return
   * @throws Exception
   */
  public static Object getNodeProperty(Node node, String property) throws Exception {
    if (!node.hasProperty(property)) {
      Neo4jGraphUtility.printNode(node);
      throw new Exception(String.format("%s does not have property %s!", node, property));
    }
    return node.getProperty(property);
  }

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
  public static String getDbNormalName(int piecesX, int piecesY, double MG, double MR, int MC,
      int MAX_HOP) {
    return String.format("graph_%s.db", getNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP));
  }

  public static String getGraphNeo4jIdMapNormalName(int piecesX, int piecesY, double MG, double MR,
      int MC, int MAX_HOP) {
    return String.format("node_map_RTree_%s.txt",
        getNormalName(piecesX, piecesY, MG, MR, MC, MAX_HOP));
  }

  public static String getNormalName(int piecesX, int piecesY, double MG, double MR, int MC,
      int MAX_HOP) {
    return String.format("%d_%d_%d_%d_%d_%d", piecesX, piecesY, (int) (MG * 100), (int) (MR * 100),
        MC, MAX_HOP);
  }


  public static ResourceIterable<Node> getNeighborsWithinHop(GraphDatabaseService service,
      Node node, RelationshipType relationshipType, int hop) {
    TraversalDescription td = service.traversalDescription().depthFirst()
        .relationships(relationshipType).evaluator(Evaluators.toDepth(hop));
    return td.traverse(node).nodes();
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
