/**
 *
 */
package edu.jhu.hlt.concrete.agiga;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import concrete.tools.AnnotationException;
import edu.jhu.agiga.AgigaDocument;
import edu.jhu.agiga.AgigaPrefs;
import edu.jhu.agiga.StreamingDocumentReader;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Constituent;
import edu.jhu.hlt.concrete.EntitySet;
import edu.jhu.hlt.concrete.Parse;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.communications.SuperCommunication;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.util.Serialization;

/**
 * @author max
 *
 */
public class ConstituentTest {

  String strPath = "src/test/resources/agiga_dog-bites-man.annotated.xml.gz";
  Path testDataPath;
  InputStream testDataIS;

  ConcreteAgigaTestingUtils catu = new ConcreteAgigaTestingUtils();

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    try {
      testDataPath = Paths.get(strPath);
      testDataIS = new FileInputStream(testDataPath.toFile());
    } catch (Exception e) {
      throw new IllegalArgumentException("You need to make sure this file exists before running the tests: " + strPath);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    testDataIS.close();
  }

  @Test
  public void testConstituentIDs() throws ConcreteException, AnnotationException, IOException {
    Communication c = catu.getCommunication(testDataPath.toString());
    SuperCommunication sc = new SuperCommunication(c);
    Tokenization t = sc.firstTokenization();
    Parse p = t.getParseList().get(0);

    assertTrue(p.getConstituentListSize() > 0);
    Set<Integer> intSet = new HashSet<>(p.getConstituentListSize());
    for (Constituent ct : p.getConstituentList()) {
      // logger.info("Got constituent ID: {}", ct.id);
      assertTrue("Duplicating constituent id " + ct.getId(), intSet.add(ct.getId()));
    }
  }

  @Test
  public void testNumEntities() throws ConcreteException, AnnotationException, IOException {
    Communication c = catu.getCommunication(testDataPath.toString());
    // SuperCommunication sc = new SuperCommunication(c);
    assertTrue("expected an entity set to be set", c.isSetEntitySetList());
    assertTrue("expected only one entity set", c.getEntitySetList().size() == 1);
    EntitySet es = c.getEntitySetList().get(0);
    assertTrue("expected 1 entity", es.getEntityList().size() == 1);
  }

//  @Test
//  public void testEntityType() throws ConcreteException, AnnotationException {
//    AgigaPrefs ap = new AgigaPrefs();
//    ap.setAll(true);
//    StreamingDocumentReader docReader = new StreamingDocumentReader(testDataPath.toString(), ap);
//    assertTrue("Cannot read a document", docReader.hasNext());
//    AgigaDocument firstDoc = docReader.next();
//    Communication c = new AgigaConverter(true).convertDoc(firstDoc);
//    // SuperCommunication sc = new SuperCommunication(c);
//    assertTrue("expected an entity set to be set", c.isSetEntitySetList());
//    assertTrue("expected only one entity set", c.getEntitySetList().size() == 1);
//    EntitySet es = c.getEntitySetList().get(0);
//    assertTrue("expected 1 entity", es.getEntityList().size() == 1);
//    Entity e = es.getEntityList().get(0);
//    assertTrue("entity type is " + e.getType() + ", not PERSON", e.getType().equals("PERSON"));
//  }

//  @Test
//  public void testMentionTypes() throws ConcreteException, AnnotationException {
//    AgigaPrefs ap = new AgigaPrefs();
//    ap.setAll(true);
//    StreamingDocumentReader docReader = new StreamingDocumentReader(testDataPath.toString(), ap);
//    assertTrue("Cannot read a document", docReader.hasNext());
//    AgigaDocument firstDoc = docReader.next();
//    Communication c = new AgigaConverter(true).convertDoc(firstDoc);
//    // SuperCommunication sc = new SuperCommunication(c);
//    assertTrue("expected an entity mention set to be set", c.isSetEntityMentionSetList());
//    assertTrue("expected only one mention entity set", c.getEntityMentionSetList().size() == 1);
//    EntityMentionSet ems = c.getEntityMentionSetList().get(0);
//    assertTrue("expected 5 entities, got " + ems.getMentionList().size(), ems.getMentionList().size() == 5);
//    int numOther = 0;
//    int numPerson = 0;
//    for (EntityMention em : ems.getMentionList()) {
//      String et = em.getEntityType();
//      if (et.equals("Unknown"))
//        numOther++;
//      else if (et.equals("PERSON"))
//        numPerson++;
//      else {
//        assertTrue("mention type is " + em.getEntityType() + ", not PERSON or Unknown", false);
//      }
//    }
//    assertTrue("saw " + numOther + " Unknown types, expected 2", numOther == 2);
//    assertTrue("saw " + numPerson + " PERSON types, expected 3", numPerson == 3);
//  }

  @Test
  public void normal() throws ConcreteException, AnnotationException, IOException {
    Communication c = catu.getCommunication(testDataPath.toString());
    new Serialization().toBytes(c);
  }
}
