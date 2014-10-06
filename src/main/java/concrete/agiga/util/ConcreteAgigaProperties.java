/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */

package concrete.agiga.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author max
 *
 */
public class ConcreteAgigaProperties {

  private final Properties props = new Properties();

  /**
   * @throws IOException
   * 
   */
  public ConcreteAgigaProperties() throws IOException {
    try (InputStream is = ConcreteAgigaProperties.class.getClassLoader().getResourceAsStream("concrete-agiga.properties");) {
      if (is == null)
        throw new IOException("Error finding concrete-agiga.properties on the classpath. Ensure it exists.");
      this.props.load(is);
    }
  }

  public String getTokenizerToolName() {
    return props.getProperty("tokenizer.name");
  }

  public String getLemmatizerToolName() {
    return props.getProperty("lemmatizer.name");
  }

  public String getPOSToolName() {
    return props.getProperty("pos-tagger.name");
  }

  public String getNERToolName() {
    return props.getProperty("ner-tagger.name");
  }

  public String getCParseToolName() {
    return props.getProperty("constituency-parser.name");
  }

  public String getDParseToolName() {
    return props.getProperty("dependency-parser.name");
  }

  public String getCorefToolName() {
    return props.getProperty("coref.name");
  }

  public final String getToolName() {
    return this.props.getProperty("tool.name");
  }
}
