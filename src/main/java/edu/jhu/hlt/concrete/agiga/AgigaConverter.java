package edu.jhu.hlt.concrete.agiga;

import edu.jhu.agiga.*;
import edu.jhu.hlt.concrete.AnnotationMetadata;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Constituent;
import edu.jhu.hlt.concrete.Dependency;
import edu.jhu.hlt.concrete.DependencyParse;
import edu.jhu.hlt.concrete.DocType;
import edu.jhu.hlt.concrete.Entity;
import edu.jhu.hlt.concrete.EntityMention;
import edu.jhu.hlt.concrete.EntityMentionSet;
import edu.jhu.hlt.concrete.EntitySet;
import edu.jhu.hlt.concrete.EntityType;
import edu.jhu.hlt.concrete.Parse;
import edu.jhu.hlt.concrete.PhraseType;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionKind;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.TaggedToken;
import edu.jhu.hlt.concrete.TextSpan;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenRefSequence;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenizationKind;
import edu.jhu.hlt.concrete.UUID;
import edu.stanford.nlp.trees.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Calendar;
import java.util.zip.GZIPOutputStream;
import java.io.*;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFileTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgigaConverter {

	public static final String toolName = "Annotated Gigaword Pipeline";
	public static final String corpusName = "Annotated Gigaword";
	public static final long annotationTime = Calendar.getInstance().getTimeInMillis() / 1000;

	
  private static final Logger logger = LoggerFactory.getLogger(AgigaConverter.class);
	
	public static AnnotationMetadata metadata() { return metadata(null); }
	public static AnnotationMetadata metadata(String addToToolName) {
		String fullToolName = toolName;
		if(addToToolName != null) fullToolName += addToToolName;
		
		AnnotationMetadata md = new AnnotationMetadata();
		md.tool = fullToolName;
		md.timestamp = annotationTime;
		md.confidence = 1f;
		return md;
	}

	public static String flattenText(AgigaDocument doc) {
		StringBuilder sb = new StringBuilder();
		for(AgigaSentence sent : doc.getSents()) {
			sb.append(flattenText(sent));
			sb.append("\n");
		}
		return sb.toString();
	}

	public static String flattenText(AgigaSentence sent) {
		StringBuilder sb = new StringBuilder();
		for(AgigaToken tok : sent.getTokens())
			sb.append(tok.getWord() + " ");
		return sb.toString().trim();
	}

	public static Parse stanford2concrete(Tree root, UUID tokenizationUUID) {
		int[] nodeCounter = new int[]{0};
		int left = 0;
		int right = root.getLeaves().size();
		/*
		// this was a bug in stanford nlp;
		// if you have a terminal with a space in it, like (CD 2 1/2)
		// stanford's getLeaves() will return Trees for 2 and 1/2
		// whereas the tokenization will have one token for 2 1/2
		// => this has since been handled in agiga, but this is a check to
		//    make sure you have the right jar
		if(root.getLeaves().size() != tokenization.getTokenList().size()) {
			System.out.println("Tokenization length = " + tokenization.getTokenList().size());
			System.out.println("Parse #leaves = " + root.getLeaves().size());
			System.out.println("tokens = " + tokenization.getTokenList());
			System.out.println("leaves = " + root.getLeaves());
			System.out.println("make sure you have the newest version of agiga!");
			throw new RuntimeException("Tokenization vs Parse error! Make sure you have the newest agiga");
		}
		*/
		
		UUID uid = new UUID();
		uid.id = java.util.UUID.randomUUID().toString();
		Parse p = new Parse();
		p.uuid = uid;
		p.metadata = metadata(" http://www.aclweb.org/anthology-new/D/D10/D10-1002.pdf");
		s2cHelper(root, 1, left, right, p);
		return p;
	}

	/**
	 * i'm using int[] as a java hack for int* (pass by reference rather than value).
	 */
	private static final HeadFinder HEAD_FINDER = new SemanticHeadFinder();
	private static int s2cHelper(Tree root, int idCounter, int left, int right, Parse p) {
		//assert(nodeCounter.length == 1);
		Constituent cb = new Constituent();
		cb.id = idCounter;
		cb.tag = root.value();
		cb.tokenSequence = extractTokenRefSequence(left, right, null, p.getUuid());

		Tree headTree = root.isLeaf() ? null : HEAD_FINDER.determineHead(root);
		int i = 0, headTreeIdx = -1;

		int leftPtr = left;
		for(Tree child : root.getChildrenAsList()) {
			int width = child.getLeaves().size();
			int childId = s2cHelper(child, idCounter++, leftPtr, leftPtr + width, p);
			cb.addToChildList(childId);
			//cb.addChild(
			leftPtr += width;
			if(headTree != null && child == headTree) {
				assert(headTreeIdx < 0);
				headTreeIdx = i;
			}
			i++;
		}

		if(headTreeIdx >= 0)
			cb.setHeadChildIndex(headTreeIdx);

		p.addToRoot(cb);
		return idCounter;
	}

	public static TokenRefSequence extractTokenRefSequence(AgigaMention m, UUID uuid) {
		return extractTokenRefSequence(m.getStartTokenIdx(), m.getEndTokenIdx(), m.getHeadTokenIdx(), uuid);
	}
	
	public static TokenRefSequence extractTokenRefSequence(int left, int right, Integer head, UUID uuid) {
	  TokenRefSequence tb = new TokenRefSequence();
	  tb.tokenizationId = new UUID(uuid.getId());

		for(int tid=left; tid<right; tid++) {
		  tb.addToTokenIndexList(tid);
			if(head != null && head == tid) {
				tb.anchorTokenIndex = tid;
			}
		}
		return tb;
	}

	/**
	 * name is the type of dependencies, e.g. "col-deps" or "col-ccproc-deps"
	 */
	public static DependencyParse convertDependencyParse(List<AgigaTypedDependency> deps, String name) {
		DependencyParse db = new DependencyParse();
		db.uuid = new UUID(java.util.UUID.randomUUID().toString());
		db.metadata = metadata(" " + name + " http://nlp.stanford.edu/software/dependencies_manual.pdf");

		for(AgigaTypedDependency ad : deps) {
		  Dependency depB = new Dependency(ad.getDepIdx());
		  depB.edgeType = ad.getType();
			
			if(ad.getGovIdx() >= 0)	// else ROOT
				depB.setGov(ad.getGovIdx());

			db.addToDependencyList(depB);
		}
		
		return db;
	}

	public static Tokenization convertTokenization(AgigaSentence sent) {

	  TokenTagging lemma = new TokenTagging();
	  lemma.setUuid(new UUID(java.util.UUID.randomUUID().toString()));
	  lemma.setMetadata(metadata());
	  
	  TokenTagging pos = new TokenTagging();
    lemma.setUuid(new UUID(java.util.UUID.randomUUID().toString()));
    lemma.setMetadata(metadata());
    
    TokenTagging ner = new TokenTagging();
    lemma.setUuid(new UUID(java.util.UUID.randomUUID().toString()));
    lemma.setMetadata(metadata());

		//TokenTagging.Builder normNerBuilder = TokenTagging.newBuilder()
		//	.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
		//	.setMetadata(metadata());

    Tokenization tb = new Tokenization();
    String uuidStr = java.util.UUID.randomUUID().toString();
    UUID tUuid = new UUID(uuidStr);
    tb.setUuid(tUuid)
      .setMetadata(metadata(" http://nlp.stanford.edu/software/tokensregex.shtml"))
      .setKind(TokenizationKind.TOKEN_LIST);
    
		int charOffset = 0;
		int tokId = 0;
		for(AgigaToken tok : sent.getTokens()) {

			int curTokId = tokId++;

			// token
			tb.addToTokenList(new Token()
				.setTokenIndex(curTokId)
				.setText(tok.getWord())
				.setTextSpan(new TextSpan()
					.setStart(charOffset)
					.setEnding(charOffset + tok.getWord().length())));

			// token annotations
			lemma.addToTaggedTokenList(makeTaggedToken(tok.getLemma(), curTokId));
			pos.addToTaggedTokenList(makeTaggedToken(tok.getPosTag(), curTokId));
			ner.addToTaggedTokenList(makeTaggedToken(tok.getNerTag(), curTokId));
			//normNerBuilder.addTaggedToken(makeTaggedToken(tok.getNormNerTag(), curTokId));

			charOffset += tok.getWord().length() + 1;
		}
		tb
			.setLemmaList(lemma)
			.setPosTagList(pos)
			.setNerTagList(ner)
			.setParse(stanford2concrete(sent.getStanfordContituencyTree(), tUuid));
		tb.addToDependencyParseList(convertDependencyParse(sent.getBasicDeps(), "basic-deps"));
		tb.addToDependencyParseList(convertDependencyParse(sent.getColDeps(), "col-deps"));
	  tb.addToDependencyParseList(convertDependencyParse(sent.getColCcprocDeps(), "col-ccproc-deps"));
	  return tb;
	}

	public static TaggedToken makeTaggedToken(String tag, int tokId) {
		return new TaggedToken()
			.setTokenIndex(tokId)
			.setTag(tag)
			.setConfidence(1f);
	}

	public static Sentence convertSentence(AgigaSentence sent, int charsFromStartOfCommunication, List<Tokenization> addTo) {
		Tokenization tokenization = convertTokenization(sent);
		addTo.add(tokenization);	// one tokenization per sentence
		return new Sentence()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
			.setTextSpan(new TextSpan()
				.setStart(charsFromStartOfCommunication)
				.setEnding(charsFromStartOfCommunication + flattenText(sent).length()))
			.setTokenization(tokenization);
	}

	public static SentenceSegmentation sentenceSegment(AgigaDocument doc, List<Tokenization> addTo) {
		SentenceSegmentation sb = new SentenceSegmentation()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
			.setMetadata(metadata(" Splitta http://www.aclweb.org/anthology-new/N/N09/N09-2061.pdf"));
		int charsFromStartOfCommunication = 0;	// communication only has one section
		for(AgigaSentence sentence : doc.getSents()) {
			sb.addToSentenceList(convertSentence(sentence, charsFromStartOfCommunication, addTo));
			charsFromStartOfCommunication += flattenText(sentence).length() + 1;	// +1 for newline at end of sentence
		}
		return sb;
	}

	public static SectionSegmentation sectionSegment(AgigaDocument doc, String rawText, List<Tokenization> addTo) {
		SectionSegmentation ss = new SectionSegmentation()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
			.setMetadata(metadata());
			ss.addToSectionList(new Section()
				.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
				.setKind(SectionKind.PASSAGE)
				.setTextSpan(new TextSpan()
					.setStart(0)
					.setEnding(rawText.length()))
				.setSentenceSegmentation((sentenceSegment(doc, addTo))));
		
		return ss;
	}

	public static String extractMentionString(AgigaMention m, AgigaDocument doc) {
		List<AgigaToken> sentence = doc.getSents().get(m.getSentenceIdx()).getTokens();
		StringBuilder sb = new StringBuilder();
		for(int i=m.getStartTokenIdx(); i<m.getEndTokenIdx(); i++) {
			sb.append(sentence.get(i).getWord());
			if(i < m.getEndTokenIdx()-1)
				sb.append(" ");
		}
		return sb.toString();
	}

	public static EntityMention convertMention(AgigaMention m, AgigaDocument doc,
			UUID corefSet, Tokenization tokenization) {
		String mstring = extractMentionString(m, doc);
		return new EntityMention()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
			.setTokens(extractTokenRefSequence(m, tokenization.getUuid()))

			.setEntityType(EntityType.UNKNOWN)
			.setPhraseType(PhraseType.NAME)	// TODO warn users that this may not be accurate
			.setConfidence(1f)
			.setText(mstring);		// TODO merge this an method below

	}

	/**
	 * adds EntityMentions to EnityMentionSet.Builder
	 * creates and returns an Entity
	 */
	public static Entity convertCoref(EntityMentionSet emsb, AgigaCoref coref, AgigaDocument doc, List<Tokenization> toks) {
		Entity entBuilder = new Entity()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()));
		for(AgigaMention m : coref.getMentions()) {
			EntityMention em = convertMention(m, doc, new UUID(java.util.UUID.randomUUID().toString()), toks.get(m.getSentenceIdx()));
			emsb.addToMentionSet(em);
			entBuilder.addToMentionIdList(em.getUuid());
		}
		
		return entBuilder;
	}

	public static Communication convertDoc(AgigaDocument doc) {
	  Communication comm = new Communication();
	  comm.id = doc.getDocId();

	  String flatText = flattenText(doc);
		List<Tokenization> toks = new ArrayList<Tokenization>();
		comm
			.setText(flatText)
			.setSectionSegmentation(sectionSegment(doc, flatText, toks))
			.setType(DocType.NEWS);
		// this must occur last so that the tokenizations have been added to toks
		EntityMentionSet emsb = new EntityMentionSet()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
			.setMetadata(metadata(" http://nlp.stanford.edu/pubs/conllst2011-coref.pdf"));
		EntitySet esb = new EntitySet()
			.setUuid(new UUID(java.util.UUID.randomUUID().toString()))
			.setMetadata(metadata(" http://nlp.stanford.edu/pubs/conllst2011-coref.pdf"));
		for(AgigaCoref coref : doc.getCorefs()) {
			Entity e = convertCoref(emsb, coref, doc, toks);
			esb.addToEntitySet(e);
		}
		
		//comm.EntityMentionSet(emsb);
		comm.setEntityMentionSet(emsb);
		comm.setEntitySet(esb);
		return comm;
	}


	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("please provide:");
			System.out.println("1 or more input Agiga XML files");
			System.out.println("an output Concrete Protobuf file");
			return;
		}
		
		long start = System.currentTimeMillis();
		File output = new File(args[args.length-1]);
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		TFileTransport ft = new TFileTransport(output.getAbsolutePath(), false);
		
		  int c = 0;
	    int step = 250;
	    for(int i=0; i<args.length-1; i++) {
	      File agigaXML = new File(args[i]);  assert(agigaXML.exists() && agigaXML.isFile());
	      StreamingDocumentReader docReader = new StreamingDocumentReader(agigaXML.getPath(), new AgigaPrefs());
	      System.out.println("reading from " + agigaXML.getPath());
	      for(AgigaDocument doc : docReader) {
	        Communication comm = convertDoc(doc);
	        //comm.writeDelimitedTo(writer);
	        byte[] commBytes = serializer.serialize(comm);
	        ft.write(commBytes);
	        
	        logger.info("Parsed a comm: " + comm.toString());
	        
	        c++;
	        if(c % step == 0) {
	          System.out.printf("wrote %d documents in %.1f sec\n",
	            c, (System.currentTimeMillis() - start)/1000d);
	        }
	      }
	    
	    
	    System.out.printf("done, wrote %d communications to %s in %.1f seconds\n",
	        c, output.getPath(), (System.currentTimeMillis() - start)/1000d);
	    
	    ft.close();
		}
	}
}

