package edu.jhu.hlt.concrete.agiga;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  /**
   * Whether or not to allow empty required lists. 
   */
  private boolean allowEmpties;

  private boolean addTextSpans;

  /**
   * @param addTextSpans Because textSpans are merely "provenance" spans, and serve
   *        merely to indicate the original span that gave rise to a particular
   *        annotation span, we don't always want to add text spans.
   * <br/>
   * This will <em>not</em> allow empty required lists.
   */
  public AgigaConverter(boolean addTextSpans) {
      this.addTextSpans = addTextSpans;
      this.allowEmpties = false;
  }

  /**
   * @param addTextSpans Because textSpans are merely "provenance" spans, and serve
   *        merely to indicate the original span that gave rise to a particular
   *        annotation span, we don't always want to add text spans.
   * @param allowEmpties Whether to allow empty required lists.
   */
  public AgigaConverter(boolean addTextSpans, boolean allowEmpties) {
      this.addTextSpans = addTextSpans;
      this.allowEmpties = allowEmpties;
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

  /**
   * Whenever there's an empty parse, this method will set the
   * required constituent list to be an empty list. It's up
   * to the caller on what to do with the returned Parse.
   */
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
    p.setUuid(this.idF.getConcreteUUID());
    p.setMetadata(metadata(" http://www.aclweb.org/anthology-new/D/D10/D10-1002.pdf"));
    s2cHelper(root, idCounter, left, right, p, tokenizationUUID);
    if(!p.isSetConstituentList()) {
        logger.warn("Setting constituent list to compensate for the empty parse for tokenization id" + tokenizationUUID + " and tree " + root);
        p.setConstituentList(new ArrayList<Constituent>());
    }
    return p;
  }

  /**
   *
   */
  private static final HeadFinder HEAD_FINDER = new SemanticHeadFinder();

  private int s2cHelper(Tree root, int[] idCounter, int left, int right, Parse p, UUID tokenizationUUID) {
    assert(idCounter.length == 1);
    Constituent cb = new Constituent();
    cb.setId(idCounter[0]++);
    cb.setTag(root.value());
    cb.setTokenSequence(extractTokenRefSequence(left, right, null, tokenizationUUID));

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
      cb.setChildList(new ArrayList<Integer>());
    return cb.id;
  }

  public TokenRefSequence extractTokenRefSequence(AgigaMention m, UUID uuid) {
    return extractTokenRefSequence(m.getStartTokenIdx(), m.getEndTokenIdx(), m.getHeadTokenIdx(), uuid);
  }

  /**
   * This creates a TokenRefSequence with provided {@code uuid}.
   * @param left The left endpoint (inclusive) of the token range.
   * @param right The right endpoint (exclusive) of the token range.
   * Note that {@code right} must be strictly greater than {@code left};
   * otherwise, a runtime exception is called.
   */
  public TokenRefSequence extractTokenRefSequence(int left, int right, Integer head, UUID uuid) {
    if(right - left <= 0) {
        throw new RuntimeException("Calling extractTokenRefSequence with right <= left: left = " + left +", right = " + right +", head = " + head +", UUID = " + uuid);
        
    }
    TokenRefSequence tb = new TokenRefSequence();
    tb.setTokenizationId(uuid);

    for (int tid = left; tid < right; tid++) {
      tb.addToTokenIndexList(tid);
      if (head != null && head == tid) {
        tb.setAnchorTokenIndex(tid);
      }
    }
    return tb;
  }

  /**
   * name is the type of dependencies, e.g. "col-deps" or "col-ccproc-deps"
   */
  public DependencyParse convertDependencyParse(List<AgigaTypedDependency> deps, String name) {
    DependencyParse db = new DependencyParse();
    db.setUuid(this.idF.getConcreteUUID());
    db.setMetadata(metadata(" " + name + " http://nlp.stanford.edu/software/dependencies_manual.pdf"));
    
    if (!deps.isEmpty()) {
      for (AgigaTypedDependency ad : deps) {
        Dependency depB = new Dependency(ad.getDepIdx());
        depB.setEdgeType(ad.getType());
  
        if (ad.getGovIdx() >= 0) // else ROOT
          depB.setGov(ad.getGovIdx());
  
        db.addToDependencyList(depB);
      }
    } else {
        db.setDependencyList(new ArrayList<Dependency>());
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
   * <br/>
   * This requires that there be tokens to process. If there are no tokens, 
   * a runtime exception is thrown.
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

    tb.setUuid(tUuid)
        .setMetadata(metadata(" http://nlp.stanford.edu/software/tokensregex.shtml"))
        .setKind(TokenizationKind.TOKEN_LIST);

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
      tl.addToTokenList(ttok);
      // token annotations
      lemma.addToTaggedTokenList(makeTaggedToken(tok.getLemma(), curTokId));
      pos.addToTaggedTokenList(makeTaggedToken(tok.getPosTag(), curTokId));
      ner.addToTaggedTokenList(makeTaggedToken(tok.getNerTag(), curTokId));
      // normNerBuilder.addTaggedToken(makeTaggedToken(tok.getNormNerTag(), curTokId));
      
      if(trustGivenOffset){
          charOffset += tok.getWord().length() + 1;
      }
    }

    if(tokId == 0) {
        throw new RuntimeException("No tokens were processed for agiga sentence " + sent);
    }
    lemma.setTaggingType("LEMMA");
    pos.setTaggingType("POS");
    ner.setTaggingType("NER");
    tb.setTokenList(tl);
    tb.addToTokenTaggingList(lemma);
    tb.addToTokenTaggingList(pos);
    tb.addToTokenTaggingList(ner);
    
    Parse parse = stanford2concrete(sent.getStanfordContituencyTree(), tUuid);
    if(!allowEmpties && !parse.isSetConstituentList()) {
        logger.warn("Not adding empty constituency parse for tokenization id " + tUuid);
    } else {
        tb.addToParseList(parse);
    }
    String[] depTypes = new String[]{"basic-deps",
                                     "col-deps",
                                     "col-ccproc-deps"};
    for(String dt : depTypes) {
        DependencyParse dp = convertDependencyParse(sent.getBasicDeps(), dt);
            if(!allowEmpties && !dp.isSetDependencyList()) {
                logger.warn("Not adding empty " + dt + " dependency parse for tokenization id " + tUuid);
            } else {
                tb.addToDependencyParseList(dp);
            }
    }
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
   * <br/>
   * A runtime exception is thrown if the provided sentence is empty.
   */
  public Sentence convertSentence(AgigaSentence sent, int charsFromStartOfCommunication, List<Tokenization> addTo) {
    if(sent != null && sent.getTokens() != null && sent.getTokens().isEmpty()) {
        throw new RuntimeException("AgigaSentence " + sent + " does not have any tokens to process");
    }
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

    SentenceSegmentation sb = new SentenceSegmentation()
      .setUuid(this.idF.getConcreteUUID())
      .setMetadata(metadata(" Stanford Sentence Splitting"));
      
    int charsFromStartOfCommunication = 0; // communication only has one section
    for (AgigaSentence sentence : doc.getSents()) {
      if(sentence.getTokens().isEmpty()) {
          logger.warn("Skipping empty sentence " + sentence + " in section with id " + sectionId);
          continue;
      }
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
    concSect.addToSentenceSegmentationList(sentenceSegment(doc, concSect.getUuid(), addTo));
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

  /**
   * Given M different NER tagging theories of a sentence with N tokens, 
   * return an M x N String array of these tags, or null if no NER theories found.
   */
  private String[][] getNETags(Tokenization tokenization) {
      int numTokens = tokenization.getTokenList().getTokenList().size();
      int numTaggings = 0;
      for(TokenTagging tt : tokenization.getTokenTaggingList()) {
          if(!tt.isSetTaggingType() || !tt.getTaggingType().equals("NER")) {
              continue;
          }
          numTaggings++;
      }
      if(numTaggings == 0){
          logger.warn("No NE Tag theories found in tokenization " + tokenization.getUuid());
          return null;
      }
      String[][] neTags = new String[numTaggings][];
      for(int i = 0; i < numTaggings; i++) {
          neTags[i] = new String[numTokens];
      }
      int which = -1;
      for(TokenTagging tt : tokenization.getTokenTaggingList()) {
          which++;
          if(!tt.isSetTaggingType() || !tt.getTaggingType().equals("NER")) {
              continue;
          }
          int whichTIndex = -1;
          for(TaggedToken tagTok : tt.getTaggedTokenList()) {
              whichTIndex++;
              if(!tagTok.isSetTokenIndex()){
                  logger.warn("token index in " + tokenization.getUuid() + " is not set");
                  continue;
              }
              if(!tagTok.isSetTag()){
                  logger.warn("token tag in " + tokenization.getUuid() + " is not set");
                  continue;
              }
              int ttIdx = tagTok.getTokenIndex();
              if(whichTIndex != ttIdx) {
                  logger.error("in tokenization " + tokenization.getUuid() + ", token index " + ttIdx + " does not match what it should (" + whichTIndex + ")");
              }
              neTags[which][ttIdx] = tagTok.getTag();
          }
      }
      return neTags;
  }

  /**
   * Returns the most common non-other entity type within a sequence.
   * This first looks at the NE type for the anchor token of the mention, if given.
   * If the anchor isn't given, then the span given by {@code em.tokens} is used.
   * The algorithm aggregates over all possible, non-OTHER NE tags for the 
   * appropriate "span."
   * The default return type is &quot;Unknown,&quot; which happens
   * <ol>
   * <li> if all tokens within {@code em.tokens} are OTHER, or </li>
   * <li> if no NE theories exist, or </li>
   * <li> if the anchor token is OTHER.</li>
   * </ol>
   */
  private String getEntityMentionType(EntityMention em, Tokenization tokenization) {
      String UNK = "Unknown";
      TokenRefSequence trs = em.getTokens();
      int anchor = trs.getAnchorTokenIndex();
      String[][] neTags = getNETags(tokenization);
      if(neTags == null) {
          return UNK;
      }
      if(neTags.length == 1 && anchor >= 0) {
          String[] slice = neTags[0];
          if(slice[anchor] == null) return UNK;
          else if(slice[anchor] == null ||
                  slice[anchor].equals("O")) return UNK;
          else return slice[anchor];
      } else {
          // we're going to iterate over a range
          int left = -1, right = -1;
          // if the anchor is given, then the range is just [x, x+1)
          if(anchor >= 0) {
              left = anchor;
              right = anchor + 1;
          } else {
              List<Integer> idxList = trs.getTokenIndexList();
              left = idxList.get(0);
              right = idxList.get(idxList.size() - 1);
          }
          Map<String, Integer> counter = new HashMap<String, Integer>();
          int maxI = -1;
          String maxType = null;
          for(int n = 0; n < neTags.length; n++){
              for(int i = left; i < right; i++) {
                  String type = neTags[n][i];
                  if(type.equals("O")) {
                      continue;
                  }
                  if(counter.get(type) == null) {
                      counter.put(type, 0);
                  }
                  int num = counter.get(type) + 1;
                  counter.put(type, num);
                  if(num > maxI) {
                      maxI = num;
                      maxType = type;
                  }
              }
          }
          return maxI>0 ? maxType : UNK;
      }
  }

  public EntityMention convertMention(AgigaMention m, AgigaDocument doc, UUID corefSet, Tokenization tokenization) {
    String mstring = extractMentionString(m, doc);

    EntityMention em = new EntityMention().setUuid(this.idF.getConcreteUUID())
        .setTokens(extractTokenRefSequence(m, tokenization.getUuid()));
    String emType = getEntityMentionType(em, tokenization);
    em.setEntityType(emType).setPhraseType("Name") // TODO warn users that this may not be accurate
        .setConfidence(1f).setText(mstring); // TODO merge this an method below
    return em;
  }

  /**
   * adds EntityMentions to EnityMentionSet.Builder creates and returns an Entity
   * This throws a runtime exception when an entity does not have any mentions AND
   * when {@code allowEmpties} is false.
   */
  public Entity convertCoref(EntityMentionSet emsb, AgigaCoref coref, AgigaDocument doc, List<Tokenization> toks) {
    if(coref.getMentions().isEmpty() && !allowEmpties) {
        throw new RuntimeException("Entity does not have any mentions");
    }
    Entity entBuilder = new Entity()
        .setUuid(this.idF.getConcreteUUID())
        .setType("Other");
    for (AgigaMention m : coref.getMentions()) {
      EntityMention em = convertMention(m, doc, this.idF.getConcreteUUID(), toks.get(m.getSentenceIdx()));
      if(m.isRepresentative()){
          String mentionString = extractMentionString(m, doc);
          entBuilder.setCanonicalName(mentionString);
      }
      emsb.addToMentionList(em);
      entBuilder.addToMentionIdList(em.getUuid());
    }
    if(!entBuilder.isSetMentionIdList()) {
        entBuilder.setMentionIdList(new ArrayList<UUID>());
    }    
    return entBuilder;
  }

  public Communication convertDoc(AgigaDocument doc) {
    Communication comm = extractRawCommunication(doc);
    List<Tokenization> toks = new ArrayList<Tokenization>();
    comm.addToSectionSegmentationList(sectionSegment(doc, comm.text, toks));
    // this must occur last so that the tokenizations have been added to toks
    List<EntityMention> mentionSet = new ArrayList<EntityMention>();
    EntityMentionSet emsb = new EntityMentionSet().setUuid(this.idF.getConcreteUUID()).setMetadata(
        metadata(" http://nlp.stanford.edu/pubs/conllst2011-coref.pdf")).setMentionList(mentionSet);
    List<Entity> entityList = new ArrayList<Entity>();
    EntitySet esb = new EntitySet().setUuid(this.idF.getConcreteUUID()).setMetadata(metadata(" http://nlp.stanford.edu/pubs/conllst2011-coref.pdf")).setEntityList(entityList);
    for (AgigaCoref coref : doc.getCorefs()) {
      Entity e = convertCoref(emsb, coref, doc, toks);
      esb.addToEntityList(e);
    }

    if(!emsb.isSetMentionList()){
        if(allowEmpties) {
            logger.warn("No mentions found: creating empty mention list");
            emsb.setMentionList(new ArrayList<EntityMention>());
            comm.addToEntityMentionSetList(emsb);
        } 
    } else {
        comm.addToEntityMentionSetList(emsb);
    }
    if(!esb.isSetEntityList()){
        if(allowEmpties) {
            logger.warn("No entities found: creating empty entity list");
            esb.setEntityList(new ArrayList<Entity>());
            comm.addToEntitySetList(esb);
        } 
    } else {
        comm.addToEntitySetList(esb);
    }
    return comm;
  }

  public Communication extractRawCommunication(AgigaDocument doc) {
    Communication comm = new Communication();
    comm.setId(doc.getDocId());
    comm.setText(flattenText(doc));
    comm.setType("News");
    comm.setUuid(this.idF.getConcreteUUID());
    AnnotationMetadata md = new AnnotationMetadata()
      .setTool("Concrete-agiga 3.3.6-SNAPSHOT")
      .setTimestamp(System.currentTimeMillis() / 1000);
    comm.setMetadata(md);
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
