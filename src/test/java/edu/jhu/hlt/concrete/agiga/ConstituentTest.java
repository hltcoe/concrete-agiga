/**
 *
 */
package edu.jhu.hlt.concrete.agiga;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.agiga.AgigaDocument;
import edu.jhu.agiga.AgigaPrefs;
import edu.jhu.agiga.StreamingDocumentReader;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Constituent;
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

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    try {
      testDataPath = Paths.get(strPath);
      testDataIS = new FileInputStream(testDataPath.toFile());
    } catch (Exception e) {
      throw new IllegalArgumentException("You need to make sure this file exists before running the tests: "
          + strPath);
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
  public void testConstituentIDs() throws ConcreteException {
      AgigaPrefs ap = new AgigaPrefs();
      ap.setAll(true);
      StreamingDocumentReader docReader = new StreamingDocumentReader(testDataPath.toString(), ap);
      assertTrue("Cannot read a document", docReader.hasNext());
      AgigaDocument firstDoc = docReader.next();
      Communication c = new AgigaConverter(true).convertDoc(firstDoc);
      SuperCommunication sc = new SuperCommunication(c);
      Tokenization t = sc.firstTokenization();
      Parse p = t.getParseList().get(0);

      assertTrue(p.getConstituentListSize() > 0);
      Set<Integer> intSet = new HashSet<>(p.getConstituentListSize());
      for (Constituent ct : p.getConstituentList()) {
          // logger.info("Got constituent ID: {}", ct.id);
          assertTrue("Duplicating constituent id " + ct.getId(),
                     intSet.add(ct.getId()));
      }
  }
  
  @Test
  public void normal() throws ConcreteException {
      AgigaPrefs ap = new AgigaPrefs();
      ap.setAll(true);
      StreamingDocumentReader docReader = new StreamingDocumentReader(testDataPath.toString(), ap);
      assertTrue(docReader.hasNext());
      AgigaDocument firstDoc = docReader.next();
      Communication c = new AgigaConverter(true).convertDoc(firstDoc);
      new Serialization().toBytes(c);
  }
}
