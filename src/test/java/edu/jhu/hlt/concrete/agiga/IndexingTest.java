/**
 *
 */
package edu.jhu.hlt.concrete.agiga;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

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
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.TaggedToken;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.communications.SuperCommunication;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.util.Serialization;

public class IndexingTest {

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

  @Test
  public void taggedTokenIndexTest() throws ConcreteException, AnnotationException, IOException {
    Communication c = catu.getCommunication(strPath);
    for(Section section : c.getSectionList()) {
      if(!section.isSetSentenceList()) continue;
      for(Sentence sentence : section.getSentenceList()) {
        if(!sentence.isSetTokenization()) continue;
        for(TokenTagging tokenTagging : sentence.getTokenization().getTokenTaggingList()) {
          int i = 0;
          assertTrue("TokenTagging " + tokenTagging.getUuid() + " does not have tagging type set",
                     tokenTagging.isSetTaggingType());
          for(TaggedToken taggedToken : tokenTagging.getTaggedTokenList()) {
            assertTrue(tokenTagging.getTaggingType() + " TokenTagging " + tokenTagging.getUuid() +
                       " does not have its " + i + "th token index set", taggedToken.isSetTokenIndex());
            assertEquals(tokenTagging.getTaggingType() + " TokenTagging " + tokenTagging.getUuid() +
                         " its " + i + "th token index set to " + taggedToken.getTokenIndex(),
                         i, taggedToken.getTokenIndex());
            i++;
          }
        }
      }
    }
  }
}
