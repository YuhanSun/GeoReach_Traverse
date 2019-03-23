package dataprocess;

import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.google.gson.JsonObject;
import commons.Util;

public class WikidataTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void getPropertyPredicateIdTest() {
    assertTrue(Wikidata.getPropertyPredicateId("http://www.wikidata.org/prop/direct/P138") == 138);
  }

  @Test
  public void decodeRowTest() {
    String string =
        "<http://www.wikidata.org/entity/Q27> <http://schema.org/name> \"Irland\"@de-at .";
    Util.println(Arrays.toString(Wikidata.decodeRow(string)));
  }

  @Test
  public void isQEntityTest() throws Exception {
    String string = "<http://www.wikidata.org/entity/Q26>";
    assertTrue(Wikidata.isQEntity(string));
    assertTrue(Wikidata.getQEntityID(string) == 26);
  }

  @Test
  public void isPropertySubjectTest() throws Exception {
    String string = "<http://www.wikidata.org/entity/P22>";
    assertTrue(Wikidata.isPropertySubject(string));
    assertTrue(Wikidata.getPropertySubjectID(string) == 22);
  }

  @Test
  public void isPropertyPredicateTest() throws Exception {
    String string = "<http://www.wikidata.org/prop/direct/P1549>";
    assertTrue(Wikidata.isPropertyPredicate(string));
    assertTrue(Wikidata.getPropertyPredicateIdReg(string) == 1549);
  }

  @Test
  public void test() {
    // Pattern p = Pattern.compile("<http://www.wikidata.org/entity/P(\\d+)>");
    // Matcher m = p.matcher("<http://www.wikidata.org/entity/P22>");
    // if (m.find()) {
    // System.out.println(m.group(0));
    // System.out.println(m.group(1));
    // }
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("id", 100);
    jsonObject.addProperty("test", "test");
    Util.println(jsonObject.toString());
    Util.println("test");
  }

}
