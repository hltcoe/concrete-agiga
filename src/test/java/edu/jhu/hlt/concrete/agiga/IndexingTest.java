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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import edu.jhu.hlt.concrete.EntityMention;
import edu.jhu.hlt.concrete.EntityMentionSet;
import edu.jhu.hlt.concrete.EntitySet;
import edu.jhu.hlt.concrete.Parse;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.TaggedToken;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenizationKind;
import edu.jhu.hlt.concrete.TokenRefSequence;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.UUID;
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

  @Test
  public void tokenListIndexTest() throws ConcreteException, AnnotationException, IOException {
    Communication c = catu.getCommunication(strPath);
    for(Section section : c.getSectionList()) {
      if(!section.isSetSentenceList()) continue;
      for(Sentence sentence : section.getSentenceList()) {
        if(!sentence.isSetTokenization()) continue;
        int i = 0;
        Tokenization tokenization = sentence.getTokenization();
        for(Token token : tokenization.getTokenList().getTokenList()) {
          assertTrue("Tokenization " + tokenization.getUuid() +
                     " does not have its " + i + "th token index set", token.isSetTokenIndex());
          assertEquals("Tokenization " + tokenization.getUuid() +
                       " its " + i + "th token index set to " + token.getTokenIndex(),
                       i, token.getTokenIndex());
          i++;
        }
      }
    }
  }

  @Test
  public void entityMentionTokenRefSequenceIndexTest() throws ConcreteException, AnnotationException, IOException {
    Communication c = catu.getCommunication(strPath);
    Map<UUID, Tokenization> tokenizationMapping = getTokenizationMapping(c);
    if(c.isSetEntityMentionSetList()) {
      for(EntityMentionSet ems : c.getEntityMentionSetList()) {
        for(EntityMention em : ems.getMentionList()) {
          TokenRefSequence trs = em.getTokens();
          Tokenization tokenization = tokenizationMapping.get(trs.getTokenizationId());
          assertTrue("No tokenization found for token ref sequence for entity mention " + em.getUuid(),
                     tokenization != null);
          verifyTokenRefSequence(trs, tokenization);
        }
      }
    }
  }

  private Map<UUID, Tokenization> getTokenizationMapping(Communication c) {
    Map<UUID, Tokenization> tokenizationMapping = new HashMap<UUID, Tokenization>();
    for(Section section : c.getSectionList()) {
      if(!section.isSetSentenceList()) continue;
      for(Sentence sentence : section.getSentenceList()) {
        if(!sentence.isSetTokenization()) continue;
        Tokenization tokenization = sentence.getTokenization();
        tokenizationMapping.put(tokenization.getUuid(), tokenization);
      }
    }
    return tokenizationMapping;
  }

  private void verifyTokenRefSequence(TokenRefSequence trs, Tokenization tokenization) {
    assertTrue("tokenization " + tokenization.getUuid() + " is not a List (is " + tokenization.getKind() + ")",
               tokenization.getKind().equals(TokenizationKind.TOKEN_LIST));
    int numTokens = tokenization.getTokenList().getTokenList().size();
    int i = 0;
    for(int tokIdx : trs.getTokenIndexList()) {
      assertTrue(i+"th token in tokenIndexList is negative: " + tokIdx,
                 tokIdx >= 0);
      assertTrue(i+"th token in tokenIndexList (tokenIndex = " + tokIdx + ")" +
                 " is beyond the range of number of tokens (" + numTokens + ")",
                 tokIdx < numTokens);
      i++;
    }
    if(trs.isSetAnchorTokenIndex()) {
      assertTrue("anchor token " + trs.getAnchorTokenIndex() + " is not in range",
                 trs.getAnchorTokenIndex() >= 0 &&
                 trs.getAnchorTokenIndex() < numTokens);
    }
  }
}
