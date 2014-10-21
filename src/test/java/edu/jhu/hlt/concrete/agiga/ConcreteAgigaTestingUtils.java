package edu.jhu.hlt.concrete.agiga;

import static org.junit.Assert.assertTrue;

import concrete.tools.AnnotationException;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.agiga.AgigaDocument;
import edu.jhu.agiga.AgigaPrefs;
import edu.jhu.agiga.StreamingDocumentReader;

import java.io.IOException;

public class ConcreteAgigaTestingUtils {
  /**
   * Load a communication from {@code filePath} using
   * all Agiga annotations (AgigaPrefs.setAll(true)).
   */
  public Communication getCommunication(String filePath) throws AnnotationException, IOException {
    AgigaPrefs ap = new AgigaPrefs();
    ap.setAll(true);
    StreamingDocumentReader docReader = new StreamingDocumentReader(filePath, ap);
    assertTrue(docReader.hasNext());
    AgigaDocument firstDoc = docReader.next();
    return new AgigaConverter(true).convertDoc(firstDoc);
  }
}
