package edu.jhu.hlt.concrete.agiga;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import concrete.agiga.util.ConcreteAgigaProperties;
import concrete.tools.AnnotationException;
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
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.TaggedToken;
import edu.jhu.hlt.concrete.TextSpan;
import edu.jhu.hlt.concrete.TheoryDependencies;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenList;
import edu.jhu.hlt.concrete.TokenRefSequence;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenizationKind;
import edu.jhu.hlt.concrete.UUID;
import edu.jhu.hlt.concrete.communications.SuperCommunication;
import edu.jhu.hlt.concrete.serialization.CommunicationSerializer;
import edu.jhu.hlt.concrete.serialization.CompactCommunicationSerializer;
import edu.jhu.hlt.concrete.util.ConcreteUUIDFactory;
import edu.jhu.hlt.concrete.validation.ValidatableTextSpan;
import edu.stanford.nlp.trees.HeadFinder;
import edu.stanford.nlp.trees.SemanticHeadFinder;
import edu.stanford.nlp.trees.Tree;

public class AgigaConverter {
  private static final Logger logger = LoggerFactory.getLogger(AgigaConverter.class);

  public static final long annotationTime = System.currentTimeMillis();

  private final ConcreteUUIDFactory idF = new ConcreteUUIDFactory();
  private final String toolName;
  private final ConcreteAgigaProperties props;

  /**
   * Whether or not to allow empty required lists.
   */
  private boolean allowEmpties;
  private boolean addTextSpans;
  private boolean storeOffsetInRaw;

  /**
   * @param addTextSpans
   *          Because textSpans are merely "provenance" spans, and serve merely to indicate the original span that gave rise to a particular annotation span, we
   *          don't always want to add text spans. <br/>
   *          This will <em>not</em> allow empty required lists.
   * @throws IOException
   */
  public AgigaConverter(boolean addTextSpans) throws IOException {
    this(addTextSpans, false);
  }

  public String getToolName() {
    return this.toolName;
  }

  /**
   * @param addTextSpans
   *          Because textSpans are merely "provenance" spans, and serve merely to indicate the original span that gave rise to a particular annotation span, we
   *          don't always want to add text spans.
   * @param allowEmpties
   *          Whether to allow empty required lists.
   * @throws IOException
   */
  public AgigaConverter(boolean addTextSpans, boolean allowEmpties) throws IOException {
    this.addTextSpans = addTextSpans;
    this.allowEmpties = allowEmpties;
    this.storeOffsetInRaw = true;
    this.props = new ConcreteAgigaProperties();
    this.toolName = this.props.getToolName();
  }

  public boolean isAddingTextSpans() {
    return addTextSpans;
  }

  public AnnotationMetadata metadata(String toolName) {
    return new AnnotationMetadata().setTool(toolName).setTimestamp(annotationTime);
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
   * Whenever there's an empty parse, this method will set the required constituent list to be an empty list. It's up to the caller on what to do with the
   * returned Parse.
   *
   * @param n is the number of tokens in the sentence
   *
   * @throws AnnotationException
   */
  public Parse stanford2concrete(Tree root, int n, UUID tokenizationUUID) throws AnnotationException {
    int left = 0;
    int right = root.getLeaves().size();
    if (right != n)
      throw new AnnotationException("number of leaves in the parse (" + right + ") is not equal to the number of tokens in the sentence (" + n + ")");
    int[] idCounter = new int[] { 0 };
    /*
     * this was a bug in stanford nlp; if you have a terminal with a space in it, like (CD 2 1/2) stanford's getLeaves() will return Trees for 2 and 1/2 whereas
     * the tokenization will have one token for 2 1/2 => this has since been handled in agiga, but this is a check to make sure you have the right jar
     */

    Parse p = new Parse();
    p.setUuid(this.idF.getConcreteUUID());
    TheoryDependencies deps = new TheoryDependencies();
    deps.addToTokenizationTheoryList(tokenizationUUID);
    AnnotationMetadata md = this.metadata(this.props.getCParseToolName()).setDependencies(deps);
    p.setMetadata(md);
    s2cHelper(root, idCounter, left, right, n, p, tokenizationUUID);
    if (!p.isSetConstituentList()) {
      logger.warn("Setting constituent list to compensate for the empty parse for tokenization id" + tokenizationUUID
          + " and tree " + root);
      p.setConstituentList(new ArrayList<Constituent>());
    }
    return p;
  }

  /**
   *
   */
  private static final HeadFinder HEAD_FINDER = new SemanticHeadFinder();

  /**
   *
   * @param root
   * @param idCounter is basically an int*, lets this recursive method update max id value
   * @param left
   * @param right
   * @param n is the length of the sentence is tokens.
   * @param p
   * @param tokenizationUUID
   * @return
   * @throws AnnotationException
   */
  private int s2cHelper(Tree root, int[] idCounter, int left, int right, int n, Parse p, UUID tokenizationUUID)
      throws AnnotationException {
    if (idCounter.length != 1)
      throw new AnnotationException("ID counter must be one, but was: " + idCounter.length);

    Constituent cb = new Constituent();
    cb.setId(idCounter[0]++);
    cb.setTag(root.value());
	cb.setStart(left);
	cb.setEnding(right);
    Tree headTree = null;
    if (!root.isLeaf()) {
      try {
        headTree = HEAD_FINDER.determineHead(root);
      } catch (java.lang.IllegalArgumentException iae) {
        logger.warn("Failed to find head, falling back on rightmost constituent.", iae);
        headTree = root.children()[root.numChildren() - 1];
      }
    }
    int i = 0, headTreeIdx = -1;

    int leftPtr = left;
    for (Tree child : root.getChildrenAsList()) {
      int width = child.getLeaves().size();
      int childId = s2cHelper(child, idCounter, leftPtr, leftPtr + width, n, p, tokenizationUUID);
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
    return cb.getId();
  }

  /**
   * In order to allow for possibly empty mentions, this will always return a validating TokenRefSequence, provided m.end &gt;= m.start. When the end points are
   * equal, the token index list will be the empty list, and a warning will be logged.
   *
   * @throws AnnotationException
   */
  public TokenRefSequence extractTokenRefSequence(AgigaMention m, UUID uuid) throws AnnotationException {
    int start = m.getStartTokenIdx();
    int end = m.getEndTokenIdx();
    if (end - start < 0)
      throw new AnnotationException("Calling extractTokenRefSequence on mention " + m + " with head = "
          + m.getHeadTokenIdx() + ", UUID = " + uuid);
    else if (end == start) {
      TokenRefSequence tb = new TokenRefSequence();
      tb.setTokenizationId(uuid).setTokenIndexList(new ArrayList<Integer>());
      if (m.getHeadTokenIdx() >= 0)
        tb.setAnchorTokenIndex(m.getHeadTokenIdx());

      logger.warn("Creating an EMPTY mention for mention " + m + " with UUID = " + uuid);
      return tb;
    }
    return extractTokenRefSequence(start, end, m.getHeadTokenIdx(), uuid);
  }

  /**
   * This creates a TokenRefSequence with provided {@link UUID}
   *
   * @param left
   *          The left endpoint (inclusive) of the token range.
   * @param right
   *          The right endpoint (exclusive) of the token range. Note that {@code right} must be strictly greater than {@code left}; otherwise, a runtime
   *          exception is called.
   * @throws AnnotationException
   */
  public TokenRefSequence extractTokenRefSequence(int left, int right, Integer head, UUID uuid)
      throws AnnotationException {
    if (right - left <= 0)
      throw new AnnotationException("Calling extractTokenRefSequence with right <= left: left = " + left + ", right = "
          + right + ", head = " + head + ", UUID = " + uuid);

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
   *
   * @param tokenizationUUID
   * @param name the name or type of the dependency parse, e.g. "basic" or "collapsed"
   *        this MUST be included in metadata for a DepenencyParse.
   */
  public DependencyParse convertDependencyParse(List<AgigaTypedDependency> deps, String name, UUID tokenizationUUID) {
    DependencyParse db = new DependencyParse();
    db.setUuid(this.idF.getConcreteUUID());
    TheoryDependencies td = new TheoryDependencies();
    td.addToTokenizationTheoryList(tokenizationUUID);
    String toolName = name + " " + this.props.getDParseToolName();
    AnnotationMetadata md = this.metadata(toolName).setDependencies(td);
    db.setMetadata(md);

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

  private AnnotationMetadata createTokenizationDependentMetadata(UUID tokenizationUuid, String addlToolName) {
    TheoryDependencies taggingDeps = new TheoryDependencies();
    taggingDeps.addToTokenizationTheoryList(tokenizationUuid);

    AnnotationMetadata md = this.metadata(addlToolName).setDependencies(taggingDeps);
    return md;
  }

  /**
   * Create a lemma-list tagging {@link AnnotationMetadata} object.
   *
   * @param tUuid
   * @return
   */
  public AnnotationMetadata getLemmaMetadata(UUID tUuid) {
    return this.createTokenizationDependentMetadata(tUuid, this.props.getLemmatizerToolName());
  }

  public AnnotationMetadata getCorefMetadata() {
    return this.metadata(this.props.getCorefToolName());
  }

  /**
   * Create a POS tagging {@link AnnotationMetadata} object.
   *
   * @param tUuid
   * @return
   */
  public AnnotationMetadata getPOSMetadata(UUID tUuid) {
    return this.createTokenizationDependentMetadata(tUuid, this.props.getPOSToolName());
  }

  /**
   * Create an NER tagging {@link AnnotationMetadata} object.
   *
   * @param tUuid
   * @return
   */
  public AnnotationMetadata getNERMetadata(UUID tUuid) {
    return this.createTokenizationDependentMetadata(tUuid, this.props.getNERToolName());
  }

  private Tokenization addTokenTaggings(AgigaSentence sent, Tokenization tkz) throws AnnotationException {
    UUID tUuid = tkz.getUuid();
    TheoryDependencies taggingDeps = new TheoryDependencies();
    taggingDeps.addToTokenizationTheoryList(tUuid);

    AnnotationMetadata lemmaMd = this.getLemmaMetadata(tUuid);
    AnnotationMetadata posMd = this.getPOSMetadata(tUuid);
    AnnotationMetadata nerMd = this.getNERMetadata(tUuid);

    TokenTagging lemma = new TokenTagging().setUuid(this.idF.getConcreteUUID()).setMetadata(lemmaMd);
    TokenTagging pos = new TokenTagging().setUuid(this.idF.getConcreteUUID()).setMetadata(posMd);
    TokenTagging ner = new TokenTagging().setUuid(this.idF.getConcreteUUID()).setMetadata(nerMd);

    lemma.setTaggingType("LEMMA");
    pos.setTaggingType("POS");
    ner.setTaggingType("NER");

    List<AgigaToken> tokList = sent.getTokens();
    int nTokens = tokList.size();
    for (int i = 0; i < nTokens; i++) {
      int curTokId = i;
      AgigaToken tok = tokList.get(i);

      lemma.addToTaggedTokenList(makeTaggedToken(tok.getLemma(), curTokId));
      pos.addToTaggedTokenList(makeTaggedToken(tok.getPosTag(), curTokId));
      ner.addToTaggedTokenList(makeTaggedToken(tok.getNerTag(), curTokId));
    }

    tkz.addToTokenTaggingList(lemma);
    tkz.addToTokenTaggingList(pos);
    tkz.addToTokenTaggingList(ner);

    Tree tree = sent.getStanfordContituencyTree();
    if (tree.getLeaves().size() != nTokens) {
      int i = 0;
      for (Tree n : tree.getLeaves())
        logger.error((i++) + ": " + n.toString());
      for (i = 0; i < nTokens; i++)
        logger.error(i + ": " + tokList.get(i).getWord());
      throw new AnnotationException("number of leaves in the parse (" + tree.getLeaves().size()
          + ") is not equal to the number of tokens in the sentence (" + nTokens + ")");
    }
    Parse parse = stanford2concrete(tree, nTokens, tUuid);
    if (!allowEmpties && !parse.isSetConstituentList())
      logger.warn("Not adding empty constituency parse for tokenization id " + tUuid);
    else
      tkz.addToParseList(parse);

    String[] depTypes = new String[] { "basic-deps", "col-deps", "col-ccproc-deps" };
    for (String dt : depTypes) {
      List<AgigaTypedDependency> deps = getDepsForType(sent, dt);
      DependencyParse dp = convertDependencyParse(deps, dt, tUuid);
      if (!allowEmpties && !dp.isSetDependencyList())
        logger.warn("Not adding empty " + dt + " dependency parse for tokenization id " + tUuid);
      else
        tkz.addToDependencyParseList(dp);
    }

    return tkz;
  }

  /**
   * Create a tokenization based on the given sentence. If we're looking to add textspans, then we will first default to using the token character offsets
   * within the sentence itself if charOffset is negative. If those are not set, then we will use the provided charOffset, as long as it is non-negative.
   * Otherwise, this will throw a runtime exception. <br/>
   * This requires that there be tokens to process. If there are no tokens, a runtime exception is thrown.
   *
   * @param preserveTokenTaggings
   *          TODO
   *
   * @throws AnnotationException
   */
  public Tokenization convertTokenization(AgigaSentence sent, int charOffset, boolean preserveTokenTaggings)
      throws AnnotationException {
    Tokenization tb = new Tokenization();
    UUID tUuid = this.idF.getConcreteUUID();

    boolean trustGivenOffset = charOffset >= 0;

    // TheoryDependencies td = new TheoryDependencies();
    // td.addToSentenceTheoryList(sentenceSegmentationUUID);

    tb.setUuid(tUuid).setKind(TokenizationKind.TOKEN_LIST);

    AnnotationMetadata md = this.metadata(this.props.getTokenizerToolName());
    tb.setMetadata(md);

    int tokId = 0;
    TokenList tl = new TokenList();
    int computedTokenStart = 0 + charOffset;
    int computedTokenEnd = computedTokenStart;
    for (AgigaToken tok : sent.getTokens()) {
      int curTokId = tokId++;

      Token ttok = new Token().setTokenIndex(curTokId).setText(tok.getWord());
      computedTokenEnd = computedTokenStart + tok.getWord().length();
      if (addTextSpans) {
        if (charOffset < 0)
          throw new AnnotationException("Bad character offset of " + charOffset + " for sentence " + sent);

        TextSpan tokTS = new TextSpan(computedTokenStart, computedTokenEnd);
        boolean isValidTokTS = new ValidatableTextSpan(tokTS).isValid();
        if (!isValidTokTS)
          throw new AnnotationException("Token TextSpan was invalid: " + tokTS.toString());
        ttok.setTextSpan(tokTS);

        if (this.storeOffsetInRaw) {
          TextSpan compTS = new TextSpan(tok.getCharOffBegin(), tok.getCharOffEnd());
          boolean isValidCompTS = new ValidatableTextSpan(compTS).isValid();
          if (!isValidCompTS)
            throw new AnnotationException("Computed/Raw TextSpan was invalid: " + compTS.toString());
          ttok.setRawTextSpan(compTS);
        }
      }

      // add 1 for spaces
      computedTokenStart = computedTokenEnd + 1;
      tl.addToTokenList(ttok);
      // token annotations
      // normNerBuilder.addTaggedToken(makeTaggedToken(tok.getNormNerTag(), curTokId));
      if (trustGivenOffset)
        charOffset += tok.getWord().length() + 1;
    }

    if (tokId == 0)
      throw new AnnotationException("No tokens were processed for agiga sentence: " + sent);

    tb.setTokenList(tl);
    if (preserveTokenTaggings)
      tb = this.addTokenTaggings(sent, tb);

    return tb;
  }

  private List<AgigaTypedDependency> getDepsForType(AgigaSentence aSent, String which) throws AnnotationException {
    switch (which) {
    case "basic-deps":
      return aSent.getBasicDeps();
    case "col-deps":
      return aSent.getColDeps();
    case "col-ccproc-deps":
      return aSent.getColCcprocDeps();
    default:
      throw new AnnotationException("Unknown dependency type: " + which);
    }
  }

  public TaggedToken makeTaggedToken(String tag, int tokId) {
    return new TaggedToken().setTokenIndex(tokId).setTag(tag);
  }

  /**
   * Create a concrete sentence based on the agiga sentence. If we're looking to add textspans, then we will first default to using the token character offsets
   * within the sentence itself if charsFromStartOfCommunication is negative. If those are not set, then we will use the provided charsFromStartOfCommunication,
   * as long as it is non-negative.
   *
   * @param preserveTokenTaggings
   *          TODO
   *
   * @throws AnnotationException
   *           if the provided sentence is empty, or if the offsets are bad.
   */
  public Sentence convertSentence(AgigaSentence sent, int charsFromStartOfCommunication, boolean preserveTokenTaggings)
      throws AnnotationException {
    if (sent != null && sent.getTokens() != null && sent.getTokens().isEmpty())
      throw new AnnotationException("AgigaSentence " + sent + " does not have any tokens to process");

    Tokenization tokenization = convertTokenization(sent, charsFromStartOfCommunication, preserveTokenTaggings);
    Sentence concSent = new Sentence().setUuid(this.idF.getConcreteUUID());
    if (addTextSpans) {
      AgigaToken firstToken = sent.getTokens().get(0);
      AgigaToken lastToken = sent.getTokens().get(sent.getTokens().size() - 1);
      if (charsFromStartOfCommunication < 0)
        throw new AnnotationException("bad character offset of " + charsFromStartOfCommunication
            + " for converting sent " + sent);

      TextSpan sentTS = new TextSpan(charsFromStartOfCommunication, charsFromStartOfCommunication
          + flattenText(sent).length());
      boolean isValidSentTS = new ValidatableTextSpan(sentTS).isValid();
      if (!isValidSentTS)
        throw new AnnotationException("TextSpan was not valid: " + sentTS.toString());
      concSent.setTextSpan(sentTS);

      if (this.storeOffsetInRaw) {
        TextSpan compTS = new TextSpan(firstToken.getCharOffBegin(), lastToken.getCharOffEnd());
        boolean isValidCompTS = new ValidatableTextSpan(compTS).isValid();
        if (!isValidCompTS)
          throw new AnnotationException("Computed TextSpan was not valid: " + compTS.toString());

        concSent.setRawTextSpan(compTS);
      }
    }

    concSent.setTokenization(tokenization);
    return concSent;
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
   * Given M different NER tagging theories of a sentence with N tokens, return an M x N String array of these tags, or null if no NER theories found.
   */
  private String[][] getNETags(Tokenization tokenization) {
    int numTokens = tokenization.getTokenList().getTokenList().size();
    int numTaggings = 0;
    for (TokenTagging tt : tokenization.getTokenTaggingList()) {
      if (!tt.isSetTaggingType() || !tt.getTaggingType().equals("NER")) {
        continue;
      }
      numTaggings++;
    }
    if (numTaggings == 0) {
      logger.warn("No NE Tag theories found in tokenization " + tokenization.getUuid());
      return null;
    }
    String[][] neTags = new String[numTaggings][];
    for (int i = 0; i < numTaggings; i++) {
      neTags[i] = new String[numTokens];
    }
    int which = -1;
    for (TokenTagging tt : tokenization.getTokenTaggingList()) {
      if (!tt.isSetTaggingType() || !tt.getTaggingType().equals("NER")) {
        continue;
      }
      which++;
      int whichTIndex = -1;
      for (TaggedToken tagTok : tt.getTaggedTokenList()) {
        whichTIndex++;
        if (!tagTok.isSetTokenIndex()) {
          logger.warn("token index in " + tokenization.getUuid() + " is not set");
          continue;
        }
        if (!tagTok.isSetTag()) {
          logger.warn("token tag in " + tokenization.getUuid() + " is not set");
          continue;
        }
        int ttIdx = tagTok.getTokenIndex();
        if (whichTIndex != ttIdx) {
          logger.error("in tokenization " + tokenization.getUuid() + ", token index " + ttIdx
              + " does not match what it should (" + whichTIndex + ")");
        }
        neTags[which][ttIdx] = tagTok.getTag();
      }
    }
    return neTags;
  }

  /**
   * Returns the most common non-other entity type within a sequence. This first looks at the NE type for the anchor token of the mention, if given. If the
   * anchor isn't given, then the span given by {@code em.tokens} is used. The algorithm aggregates over all possible, non-OTHER NE tags for the appropriate
   * "span." The default return type is &quot;Unknown,&quot; which happens
   * <ol>
   * <li>if all tokens within {@code em.tokens} are OTHER, or</li>
   * <li>if no NE theories exist, or</li>
   * <li>if the anchor token is OTHER,</li>
   * <li>if the mention is empty (so the token index list is the empty list).</li>
   * </ol>
   */
  private String getEntityMentionType(EntityMention em, Tokenization tokenization) {
    String UNK = "Unknown";
    TokenRefSequence trs = em.getTokens();
    int anchor = trs.getAnchorTokenIndex();
    String[][] neTags = getNETags(tokenization);
    if (neTags == null || trs.getTokenIndexList().size() == 0) {
      return UNK;
    }
    if (neTags.length == 1 && anchor >= 0) {
      String[] slice = neTags[0];
      if (slice[anchor] == null)
        return UNK;
      else if (slice[anchor] == null || slice[anchor].equals("O"))
        return UNK;
      else
        return slice[anchor];
    } else {
      // we're going to iterate over a range
      int left = -1, right = -1;
      // if the anchor is given, then the range is just [x, x+1)
      if (anchor >= 0) {
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
      for (int n = 0; n < neTags.length; n++) {
        for (int i = left; i < right; i++) {
          String type = neTags[n][i];
          if (type.equals("O")) {
            continue;
          }
          if (counter.get(type) == null) {
            counter.put(type, 0);
          }
          int num = counter.get(type) + 1;
          counter.put(type, num);
          if (num > maxI) {
            maxI = num;
            maxType = type;
          }
        }
      }
      return maxI > 0 ? maxType : UNK;
    }
  }

  public EntityMention convertMention(AgigaMention m, AgigaDocument doc, UUID corefSet, Tokenization tokenization)
      throws AnnotationException {
    String mstring = extractMentionString(m, doc);
    TokenRefSequence trs = extractTokenRefSequence(m, tokenization.getUuid());
    EntityMention em = new EntityMention().setUuid(this.idF.getConcreteUUID()).setTokens(trs);
    // String emType = getEntityMentionType(em, tokenization);
    em.setText(mstring); // TODO merge this an method below
    return em;
  }

  /**
   * adds EntityMentions to EnityMentionSet.Builder creates and returns an Entity This throws a runtime exception when an entity does not have any mentions AND
   * when {@code allowEmpties} is false. The entity type is set to be the type of the representative mention if either the representative mention is the only
   * mention with a set type, or if the most frequently seen mention type is the same as the representative mention type. Otherwise, the entity is assigned type
   * "Other," and a notice is logged.
   */
  public Entity convertCoref(EntityMentionSet emsb, AgigaCoref coref, AgigaDocument doc, List<Tokenization> toks)
      throws AnnotationException {
    if (coref.getMentions().isEmpty() && !allowEmpties)
      throw new AnnotationException("Entity does not have any mentions");

    Entity entBuilder = new Entity().setUuid(this.idF.getConcreteUUID());
    // Map<String, Integer> counter = new HashMap<String, Integer>();
    // int maxI = -1;
    // String maxEType = null;
    // String repEntType = null;

    for (AgigaMention m : coref.getMentions()) {
      Tokenization tz = toks.get(m.getSentenceIdx());
      EntityMention em = convertMention(m, doc, this.idF.getConcreteUUID(), tz);
      if (m.isRepresentative()) {
        String mentionString = extractMentionString(m, doc);
        entBuilder.setCanonicalName(mentionString);
        // repEntType = em.getEntityType();
        // }
        // if (!counter.containsKey(em.getEntityType())) {
        // counter.put(em.getEntityType(), 0);
        // }
        // int num = counter.get(em.getEntityType()) + 1;
        // counter.put(em.getEntityType(), num);
        // if (num > maxI) {
        // maxI = num;
        // maxEType = em.getEntityType();
      }

      emsb.addToMentionList(em);
      entBuilder.addToMentionIdList(em.getUuid());
    }

    // if (maxEType != null && repEntType != null && repEntType.equals(maxEType)) {
    // entBuilder.setType(repEntType);
    // } else {
    // logger.debug("For entity " + entBuilder.getUuid() + ", the representative entity type " + repEntType + " isn't the same as the max seen mention type "
    // + maxEType + "; setting entity type to Other");
    // entBuilder.setType("Other");
    // }

    if (!entBuilder.isSetMentionIdList())
      entBuilder.setMentionIdList(new ArrayList<UUID>());

    return entBuilder;
  }

  public Communication convertDoc(AgigaDocument doc) throws AnnotationException {
    Communication comm = extractRawCommunication(doc);

    // Section the communication.
    String commText = comm.getText();
    Section concSect = new Section(this.idF.getConcreteUUID(), "Passage");
    if (addTextSpans)
      concSect.setTextSpan(new TextSpan().setStart(0).setEnding(commText.length()));
    comm.addToSectionList(concSect);

    // Perform sentence splitting.
    int charsFromStartOfCommunication = 0; // communication only has one section
    for (AgigaSentence sentence : doc.getSents()) {
      if (sentence.getTokens().isEmpty()) {
        logger.warn("Skipping empty sentence " + sentence + " in section with id " + concSect.getUuid());
        continue;
      }

      Sentence st = this.convertSentence(sentence, charsFromStartOfCommunication, true);
      concSect.addToSentenceList(st);
      // sb.addToSentenceList(convertSentence(sentence, charsFromStartOfCommunication, addTo, sectionSegmentationUUID));
      charsFromStartOfCommunication += flattenText(sentence).length() + 1; // +1 for newline at end of sentence
    }

    // Retrieve the tokenizations.
    Collection<Tokenization> tokColl = new SuperCommunication(comm).generateTokenizationIdToTokenizationMap().values();
    List<Tokenization> toks = new ArrayList<>(tokColl);
    List<EntityMention> mentionSet = new ArrayList<EntityMention>();
    AnnotationMetadata md = this.getCorefMetadata();
    EntityMentionSet emsb = new EntityMentionSet().setUuid(this.idF.getConcreteUUID()).setMetadata(md)
        .setMentionList(mentionSet);
    List<Entity> entityList = new ArrayList<Entity>();
    EntitySet esb = new EntitySet().setUuid(this.idF.getConcreteUUID()).setMetadata(md).setEntityList(entityList);
    for (AgigaCoref coref : doc.getCorefs()) {
      Entity e = convertCoref(emsb, coref, doc, toks);
      esb.addToEntityList(e);
    }

    if (!emsb.isSetMentionList()) {
      if (allowEmpties) {
        logger.warn("No mentions found: creating empty mention list");
        emsb.setMentionList(new ArrayList<EntityMention>());
      }
    }
    comm.addToEntityMentionSetList(emsb);

    if (!esb.isSetEntityList()) {
      if (allowEmpties) {
        logger.warn("No entities found: creating empty entity list");
        esb.setEntityList(new ArrayList<Entity>());
      }
    }
    comm.addToEntitySetList(esb);

    return comm;
  }

  public Communication extractRawCommunication(AgigaDocument doc) {
    Communication comm = new Communication();
    comm.setId(doc.getDocId());
    comm.setText(flattenText(doc));
    comm.setType("News");
    comm.setUuid(this.idF.getConcreteUUID());
    AnnotationMetadata md = new AnnotationMetadata().setTool(this.toolName).setTimestamp(
        System.currentTimeMillis() / 1000);
    comm.setMetadata(md);
    return comm;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Please provide at minimum: ");
      System.out.println("Path to a directory for Concrete thrift output files");
      System.out
          .println("A boolean to indicate whether to extract ONLY the raw Concrete Communications (e.g., whether drop annotations or not)");
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

    CommunicationSerializer cs = new CompactCommunicationSerializer();
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

          byte[] commBytes = cs.toBytes(comm);
          fos.write(commBytes);

          c++;
          if (c % step == 0) {
            logger.info("Wrote {} documents in {} seconds.", c, (System.currentTimeMillis() - start) / 1000d);
          }
        }
      }

      logger.info("Finished. Wrote {} communications to {} in {} seconds.", c, outputDir.getPath(),
          (System.currentTimeMillis() - start) / 1000d);
    }
  }
}
