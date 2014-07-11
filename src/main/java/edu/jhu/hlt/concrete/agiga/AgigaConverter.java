package edu.jhu.hlt.concrete.agiga;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.agiga.AgigaCoref;
import edu.jhu.agiga.AgigaDocument;
import edu.jhu.agiga.AgigaMention;
import edu.jhu.agiga.AgigaPrefs;
import edu.jhu.agiga.AgigaSentence;
import edu.jhu.agiga.AgigaToken;
import edu.jhu.agiga.AgigaTypedDependency;
import edu.jhu.agiga.StreamingDocumentReader;
import edu.jhu.hlt.concrete.AnnotationMetadata;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Constituent;
import edu.jhu.hlt.concrete.Dependency;
import edu.jhu.hlt.concrete.DependencyParse;
import edu.jhu.hlt.concrete.Entity;
import edu.jhu.hlt.concrete.EntityMention;
import edu.jhu.hlt.concrete.EntityMentionSet;
import edu.jhu.hlt.concrete.EntitySet;
import edu.jhu.hlt.concrete.Parse;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.TaggedToken;
import edu.jhu.hlt.concrete.TextSpan;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenList;
import edu.jhu.hlt.concrete.TokenRefSequence;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenizationKind;
import edu.jhu.hlt.concrete.UUID;
import edu.jhu.hlt.concrete.util.ConcreteUUIDFactory;
import edu.stanford.nlp.trees.HeadFinder;
import edu.stanford.nlp.trees.SemanticHeadFinder;
import edu.stanford.nlp.trees.Tree;

public class AgigaConverter {

  public static final String toolName = "Annotated Gigaword Pipeline";
  public static final String corpusName = "Annotated Gigaword";
  public static final long annotationTime = System.currentTimeMillis();

  private static final Logger logger = LoggerFactory.getLogger(AgigaConverter.class);
  
  private final ConcreteUUIDFactory idF = new ConcreteUUIDFactory();

  private boolean addTextSpans;

  /**
   * @param addTextSpans Because textSpans are merely "provenance" spans, and serve
   *        merely to indicate the original span that gave rise to a particular
   *        annotation span, we don't always want to add text spans.
   */
  public AgigaConverter(boolean addTextSpans) {
      this.addTextSpans = addTextSpans;
  }

  public boolean isAddingTextSpans(){
      return addTextSpans;
  }

  public AnnotationMetadata metadata() {
    return metadata(null);
  }

  public AnnotationMetadata metadata(String addToToolName) {
    String fullToolName = toolName;
    if (addToToolName != null)
      fullToolName += addToToolName;

    AnnotationMetadata md = new AnnotationMetadata();
    md.setTool(fullToolName);
    md.setTimestamp(annotationTime);
    md.setConfidence(1f);
    return md;
  }

  public String flattenText(AgigaDocument doc) {
    StringBuilder sb = new StringBuilder();
    for (AgigaSentence sent : doc.getSents()) {
      sb.append(flattenText(sent));
      sb.append("\n");
    }
    return sb.toString();
  }

  public String flattenText(AgigaSentence sent) {
    StringBuilder sb = new StringBuilder();
    for (AgigaToken tok : sent.getTokens())
      sb.append(tok.getWord() + " ");
    return sb.toString().trim();
  }

  public Parse stanford2concrete(Tree root, UUID tokenizationUUID) {
    int left = 0;
    int right = root.getLeaves().size();
    int[] idCounter = new int[]{0};
    /*
     * this was a bug in stanford nlp; if you have a terminal with a space in it, like (CD 2 1/2)
     * stanford's getLeaves() will return Trees for 2 and 1/2
     * whereas the tokenization will have one token for 2 1/2
     * => this has since been handled in agiga, but this is a check to
     * make sure you have the right jar
     */

    Parse p = new Parse();
    p.uuid = this.idF.getConcreteUUID();
    p.metadata = metadata(" http://www.aclweb.org/anthology-new/D/D10/D10-1002.pdf");
    s2cHelper(root, idCounter, left, right, p, tokenizationUUID);
    return p;
  }

  /**
   *
   */
  private static final HeadFinder HEAD_FINDER = new SemanticHeadFinder();

  private int s2cHelper(Tree root, int[] idCounter, int left, int right, Parse p, UUID tokenizationUUID) {
    assert(idCounter.length == 1);
    Constituent cb = new Constituent();
    cb.id = idCounter[0]++;
    cb.tag = root.value();
    cb.tokenSequence = extractTokenRefSequence(left, right, null, tokenizationUUID);

    Tree headTree = root.isLeaf() ? null : HEAD_FINDER.determineHead(root);
    int i = 0, headTreeIdx = -1;

    int leftPtr = left;
    for (Tree child : root.getChildrenAsList()) {
      int width = child.getLeaves().size();
      int childId = s2cHelper(child, idCounter, leftPtr, leftPtr + width, p, tokenizationUUID);
      cb.addToChildList(childId);

      leftPtr += width;
      if (headTree != null && child == headTree) {
        assert (headTreeIdx < 0);
        headTreeIdx = i;
      }
      i++;
    }

    if (headTreeIdx >= 0)
      cb.setHeadChildIndex(headTreeIdx);

    p.addToConstituentList(cb);
    if (!cb.isSetChildList())
      cb.childList = new ArrayList<>();
    return cb.id;
  }

  public TokenRefSequence extractTokenRefSequence(AgigaMention m, UUID uuid) {
    return extractTokenRefSequence(m.getStartTokenIdx(), m.getEndTokenIdx(), m.getHeadTokenIdx(), uuid);
  }

  public TokenRefSequence extractTokenRefSequence(int left, int right, Integer head, UUID uuid) {
    TokenRefSequence tb = new TokenRefSequence();
    tb.tokenizationId = uuid;

    for (int tid = left; tid < right; tid++) {
      tb.addToTokenIndexList(tid);
      if (head != null && head == tid) {
        tb.anchorTokenIndex = tid;
      }
    }
    return tb;
  }

  /**
   * name is the type of dependencies, e.g. "col-deps" or "col-ccproc-deps"
   */
  public DependencyParse convertDependencyParse(List<AgigaTypedDependency> deps, String name) {
    DependencyParse db = new DependencyParse();
    db.uuid = this.idF.getConcreteUUID();
    db.metadata = metadata(" " + name + " http://nlp.stanford.edu/software/dependencies_manual.pdf");
    
    if (!deps.isEmpty()) {
      for (AgigaTypedDependency ad : deps) {
        Dependency depB = new Dependency(ad.getDepIdx());
        depB.edgeType = ad.getType();
  
        if (ad.getGovIdx() >= 0) // else ROOT
          depB.setGov(ad.getGovIdx());
  
        db.addToDependencyList(depB);
      }
    } else {
      db.dependencyList = new ArrayList<Dependency>();
    }
    
    return db;
  }

  /**
   * Create a tokenization based on the given sentence. If we're looking to add
   * textspans, then we will first default to using the token character offsets
   * within the sentence itself if charOffset is negative. 
   * If those are not set, then we will use the 
   * provided charOffset, as long as it is non-negative. Otherwise, this will
   * throw a runtime exception.
   */
  public Tokenization convertTokenization(AgigaSentence sent, int charOffset) {
    TokenTagging lemma = new TokenTagging();
    lemma.setUuid(this.idF.getConcreteUUID());
    lemma.setMetadata(metadata());

    TokenTagging pos = new TokenTagging();
    pos.setUuid(this.idF.getConcreteUUID());
    pos.setMetadata(metadata());

    TokenTagging ner = new TokenTagging();
    ner.setUuid(this.idF.getConcreteUUID());
    ner.setMetadata(metadata());

    // TokenTagging.Builder normNerBuilder = TokenTagging.newBuilder()
    // .setUuid(new UUID(java.util.UUID.randomUUID().toString()))
    // .setMetadata(metadata());

    Tokenization tb = new Tokenization();
    UUID tUuid = this.idF.getConcreteUUID();

    boolean trustGivenOffset = charOffset >= 0;

    tb.setUuid(tUuid).setMetadata(metadata(" http://nlp.stanford.edu/software/tokensregex.shtml")).setKind(TokenizationKind.TOKEN_LIST);

    int tokId = 0;
    TokenList tl = new TokenList();
    for (AgigaToken tok : sent.getTokens()) {
      int curTokId = tokId++;

      Token ttok = new Token().setTokenIndex(curTokId).setText(tok.getWord());
      if(addTextSpans) {
          if(charOffset < 0 && 
             tok.getCharOffBegin() >= 0 && tok.getCharOffEnd() > tok.getCharOffBegin()){
              ttok.setTextSpan(new TextSpan()
                               .setStart(tok.getCharOffBegin())
                               .setEnding(tok.getCharOffEnd()));
          } else {
              if(charOffset < 0){
                  throw new RuntimeException("Bad character offset of " + charOffset + " for sentence " + sent);
              }
              ttok.setTextSpan(new TextSpan()
                               .setStart(charOffset)
                               .setEnding(charOffset + tok.getWord().length()));
          }
      }
      tl.addToTokens(ttok);
      // token annotations
      lemma.addToTaggedTokenList(makeTaggedToken(tok.getLemma(), curTokId));
      pos.addToTaggedTokenList(makeTaggedToken(tok.getPosTag(), curTokId));
      ner.addToTaggedTokenList(makeTaggedToken(tok.getNerTag(), curTokId));
      // normNerBuilder.addTaggedToken(makeTaggedToken(tok.getNormNerTag(), curTokId));
      
      if(trustGivenOffset){
          charOffset += tok.getWord().length() + 1;
      }
    }

    tb.setTokenList(tl);
    
    tb.setLemmaList(lemma).setPosTagList(pos).setNerTagList(ner).setParse(stanford2concrete(sent.getStanfordContituencyTree(), tUuid));
    tb.addToDependencyParseList(convertDependencyParse(sent.getBasicDeps(), "basic-deps"));
    tb.addToDependencyParseList(convertDependencyParse(sent.getColDeps(), "col-deps"));
    tb.addToDependencyParseList(convertDependencyParse(sent.getColCcprocDeps(), "col-ccproc-deps"));
    return tb;
  }

  public TaggedToken makeTaggedToken(String tag, int tokId) {
    return new TaggedToken()
      .setTokenIndex(tokId)
      .setTag(tag)
      .setConfidence(1f);
  }

  /**
   * Create a concrete sentence based on the agiga sentence. If we're looking to add
   * textspans, then we will first default to using the token character offsets
   * within the sentence itself if charsFromStartOfCommunication is negative. 
   * If those are not set, then we will use the 
   * provided charsFromStartOfCommunication, as long as it is non-negative. 
   * Otherwise, this will throw a runtime exception.
   */
  public Sentence convertSentence(AgigaSentence sent, int charsFromStartOfCommunication, List<Tokenization> addTo) {
    Tokenization tokenization = convertTokenization(sent, charsFromStartOfCommunication);
    addTo.add(tokenization); // one tokenization per sentence
    Sentence concSent = new Sentence().setUuid(this.idF.getConcreteUUID());
    if(addTextSpans){
        AgigaToken firstToken = sent.getTokens().get(0);
        AgigaToken lastToken  = sent.getTokens().get(sent.getTokens().size() - 1);
        if(charsFromStartOfCommunication < 0 && 
           firstToken.getCharOffBegin() >= 0 && lastToken.getCharOffEnd() > firstToken.getCharOffBegin()){
            concSent.setTextSpan(new TextSpan()
                                 .setStart(firstToken.getCharOffBegin())
                                 .setEnding(lastToken.getCharOffEnd()));
        } else {
            if(charsFromStartOfCommunication < 0){
                throw new RuntimeException("bad character offset of " + charsFromStartOfCommunication + " for converting sent " + sent);
            }
            concSent.setTextSpan(new TextSpan()
                                 .setStart(charsFromStartOfCommunication)
                                 .setEnding(charsFromStartOfCommunication + flattenText(sent).length()));
        }
    }
    concSent.addToTokenizationList(tokenization);
    return concSent;
  }

  public SentenceSegmentation sentenceSegment(AgigaDocument doc, UUID sectionId, List<Tokenization> addTo) {

    SentenceSegmentation sb = new SentenceSegmentation().setUuid(this.idF.getConcreteUUID()).setMetadata(
        metadata(" Splitta http://www.aclweb.org/anthology-new/N/N09/N09-2061.pdf")).setSectionId(sectionId);
    int charsFromStartOfCommunication = 0; // communication only has one section
    for (AgigaSentence sentence : doc.getSents()) {
      sb.addToSentenceList(convertSentence(sentence, charsFromStartOfCommunication, addTo));
      charsFromStartOfCommunication += flattenText(sentence).length() + 1; // +1 for newline at end of sentence
    }
    return sb;
  }

  /**
   * Note: this assumes that it will be called only once: that is, that there is only one 
   * section in the entire agiga document. Therefore, the provenance span will span the 
   * entire text.
   */
  public SectionSegmentation sectionSegment(AgigaDocument doc, String rawText, List<Tokenization> addTo) {

    SectionSegmentation ss = new SectionSegmentation().setUuid(this.idF.getConcreteUUID()).setMetadata(metadata());
    Section concSect = new Section()
        .setUuid(this.idF.getConcreteUUID())
        .setKind("Passage");
    if(addTextSpans){
        concSect.setTextSpan(new TextSpan()
                             .setStart(0)
                             .setEnding(rawText.length()));
    }
    concSect.addToSentenceSegmentation(sentenceSegment(doc, concSect.getUuid(), addTo));
    ss.addToSectionList(concSect);
    return ss;
  }

  public String extractMentionString(AgigaMention m, AgigaDocument doc) {
    List<AgigaToken> sentence = doc.getSents().get(m.getSentenceIdx()).getTokens();
    StringBuilder sb = new StringBuilder();
    for (int i = m.getStartTokenIdx(); i < m.getEndTokenIdx(); i++) {
      sb.append(sentence.get(i).getWord());
      if (i < m.getEndTokenIdx() - 1)
        sb.append(" ");
    }
    return sb.toString();
  }

  public EntityMention convertMention(AgigaMention m, AgigaDocument doc, UUID corefSet, Tokenization tokenization) {
    String mstring = extractMentionString(m, doc);

    return new EntityMention().setUuid(this.idF.getConcreteUUID()).setTokens(extractTokenRefSequence(m, tokenization.getUuid()))
    .setEntityType("Unknown").setPhraseType("Name") // TODO warn users that this may not be accurate
        .setConfidence(1f).setText(mstring); // TODO merge this an method below

  }

  /**
   * adds EntityMentions to EnityMentionSet.Builder creates and returns an Entity
   */
  public Entity convertCoref(EntityMentionSet emsb, AgigaCoref coref, AgigaDocument doc, List<Tokenization> toks) {

    Entity entBuilder = new Entity()
      .setUuid(this.idF.getConcreteUUID())
      .setType("Other");
    for (AgigaMention m : coref.getMentions()) {
      EntityMention em = convertMention(m, doc, this.idF.getConcreteUUID(), toks.get(m.getSentenceIdx()));
      emsb.addToMentionSet(em);
      entBuilder.addToMentionIdList(em.getUuid());
    }

    return entBuilder;
  }

  public Communication convertDoc(AgigaDocument doc) {
    Communication comm = extractRawCommunication(doc);
    List<Tokenization> toks = new ArrayList<Tokenization>();
    comm.addToSectionSegmentations(sectionSegment(doc, comm.text, toks));
    // this must occur last so that the tokenizations have been added to toks
    List<EntityMention> mentionSet = new ArrayList<EntityMention>();
    EntityMentionSet emsb = new EntityMentionSet().setUuid(this.idF.getConcreteUUID()).setMetadata(
        metadata(" http://nlp.stanford.edu/pubs/conllst2011-coref.pdf")).setMentionSet(mentionSet);
    List<Entity> entityList = new ArrayList<Entity>();
    EntitySet esb = new EntitySet().setUuid(this.idF.getConcreteUUID()).setMetadata(metadata(" http://nlp.stanford.edu/pubs/conllst2011-coref.pdf")).setEntityList(entityList);
    for (AgigaCoref coref : doc.getCorefs()) {
      Entity e = convertCoref(emsb, coref, doc, toks);
      esb.addToEntityList(e);
    }

    // comm.EntityMentionSet(emsb);
    comm.addToEntityMentionSets(emsb);
    comm.addToEntitySets(esb);
    return comm;
  }

  public Communication extractRawCommunication(AgigaDocument doc) {
    Communication comm = new Communication();
    comm.id = doc.getDocId();
    comm.text = flattenText(doc);
    comm.type = "News";
    comm.uuid = this.idF.getConcreteUUID();
    return comm;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Please provide at minimum: ");
      System.out.println("Path to a directory for Concrete thrift output files");
      System.out.println("A boolean to indicate whether to extract ONLY the raw Concrete Communications (e.g., whether drop annotations or not)");
      System.out.println("Path to 1 or more input Agiga XML files");
      System.out.println("e.g., " + AgigaConverter.class.getSimpleName() + " /my/output/dir true /my/agiga/doc.xml.gz");
      System.exit(1);
    }

    String rawExtractionString = args[1];
    boolean rawExtraction = Boolean.parseBoolean(rawExtractionString);
    if (rawExtraction)
        logger.info("Extracting only raw Agiga documents.");
    else
        logger.info("Extracting Agiga documents and annotations.");

    String outputDirPath = args[0];
    File outputDir = new File(outputDirPath);
    if (!outputDir.exists())
      outputDir.mkdir();
    logger.info("Writing output to: " + outputDirPath);

    long start = System.currentTimeMillis();

    boolean addTextSpans = true;

    AgigaConverter ac = new AgigaConverter(addTextSpans);

    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());

    int c = 0;
    int step = 1000;
    for (int i = 2; i < args.length; i++) {
      File agigaXML = new File(args[i]);
      if (!agigaXML.exists()) {
        logger.error("File: {} does not seem to exist.", agigaXML.getAbsolutePath());
        continue;
      } else if (!agigaXML.isFile()) {
        logger.error("File: {} does not seem to be a file.", agigaXML.getAbsolutePath());
        continue;
      }

      StreamingDocumentReader docReader = new StreamingDocumentReader(agigaXML.getPath(), new AgigaPrefs());
      logger.info("Reading from: " + agigaXML.getPath());

      for (AgigaDocument doc : docReader) {
        String outFilePath = outputDir + File.separator + doc.getDocId() + ".thrift";
        File outFile = new File(outFilePath);
        if (outFile.exists())
            outFile.delete();
        outFile.createNewFile();
        try (FileOutputStream fos = new FileOutputStream(outFile)) {
          Communication comm;
          if (rawExtraction)
            comm = ac.extractRawCommunication(doc);
          else
            comm = ac.convertDoc(doc);

          byte[] commBytes = serializer.serialize(comm);
          fos.write(commBytes);

          c++;
          if (c % step == 0) {
            logger.info("Wrote {} documents in {} seconds.", c, (System.currentTimeMillis() - start) / 1000d);
          }
        }
      }

      logger.info("Finished. Wrote {} communications to {} in {} seconds.", c, outputDir.getPath(), (System.currentTimeMillis() - start) / 1000d);

    }
  }
}
