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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.agiga.AgigaDocument;
import edu.jhu.agiga.AgigaPrefs;
import edu.jhu.agiga.StreamingDocumentReader;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Constituent;
import edu.jhu.hlt.concrete.Parse;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.util.SuperCommunication;

/**
 * @author max
 *
 */
public class ConstituentTest {

  private static final Logger logger = LoggerFactory.getLogger(ConstituentTest.class);
  
  String strPath = "src/test/resources/afp_eng_199405.xml.gz";
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
    StreamingDocumentReader docReader = new StreamingDocumentReader(testDataPath.toString(), new AgigaPrefs());
    assertTrue(docReader.hasNext());
    AgigaDocument firstDoc = docReader.next();
    Communication c = AgigaConverter.convertDoc(firstDoc);
    SuperCommunication sc = new SuperCommunication(c);
    Tokenization t = sc.firstTokenization();
    Parse p = t.getParse();
    
    Set<Integer> intSet = new HashSet<>(t.getParse().getConstituentListSize());
    for (Constituent ct : p.getConstituentList()) {
      // logger.info("Got constituent ID: {}", ct.id);
      assertTrue(intSet.add(ct.getId()));
    }
  }

}
