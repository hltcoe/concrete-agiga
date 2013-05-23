package edu.jhu.concrete.agiga;

import edu.jhu.concrete.Concrete.*;
import edu.jhu.concrete.util.*;
import edu.jhu.concrete.io.ProtocolBufferWriter;
import edu.jhu.agiga.*;
import edu.stanford.nlp.trees.Tree;
import java.util.*;
import java.io.*;

// TODO parse converters
// TODO coref chains
// TODO pos, ner tags and lemmas
class AgigaConverter {

	public static final String toolName = "Annotated Gigaword Pipeline";
	public static final String corpusName = "Annotated Gigaword";
	public static final double annotationTime = Calendar.getInstance().getTimeInMillis() / 1000d;

	public static AnnotationMetadata metadata() {
		return AnnotationMetadata.newBuilder()
			.setTool(toolName)
			.setTimestamp(annotationTime)
			.setConfidence(1f)
			.build();
	}

	public static String flattenText(AgigaDocument doc) {
		StringBuilder sb = new StringBuilder();
		for(AgigaSentence sent : doc.getSents())
			sb.append(flattenText(sent));
		return sb.toString();
	}

	public static String flattenText(AgigaSentence sent) {
		StringBuilder sb = new StringBuilder();
		for(AgigaToken tok : sent.getTokens())
			sb.append(tok.getWord() + " ");
		return sb.toString().trim();
	}

	// TODO
	public static Parse stanford2concrete(Tree root) {
		//throw new RuntimeException("implement me!");
		return Parse.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setMetadata(metadata())
			.build();
	}

	// TODO
	public static Parse convertDependencyParse(List<AgigaTypedDependency> deps, String name) {
		//throw new RuntimeException("implement me!");
		return Parse.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setMetadata(metadata())
			.build();
	}

	public static Tokenization convertTokenization(AgigaSentence sent) {
		Tokenization.Builder tb = Tokenization.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setMetadata(metadata())
			.setKind(Tokenization.Kind.TOKEN_LIST);
		int charOffset = 0;
		int tokId = 0;
		for(AgigaToken tok : sent.getTokens()) {
			tb.addToken(Token.newBuilder()
				.setTokenId(tokId++)
				.setText(tok.getWord())
				.setTextSpan(TextSpan.newBuilder()
					.setStart(charOffset)
					.setEnd(charOffset + tok.getWord().length())
					.build())
				.build());
			charOffset += tok.getWord().length() + 1;
		}
		return tb.build();
	}

	public static Sentence convertSentence(AgigaSentence sent) {
		return Sentence.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setTextSpan(TextSpan.newBuilder()
				.setStart(0)
				.setEnd(flattenText(sent).length())
				.build())
			// tokenization
			.addTokenization(convertTokenization(sent))
			// parse
			.addParse(stanford2concrete(sent.getStanfordContituencyTree()))
			.addParse(convertDependencyParse(sent.getBasicDeps(), "basic-deps"))
			.addParse(convertDependencyParse(sent.getColDeps(), "col-deps"))
			.addParse(convertDependencyParse(sent.getColCcprocDeps(), "col-ccproc-deps"))
			.build();
	}

	public static SentenceSegmentation sentenceSegment(AgigaDocument doc) {
		SentenceSegmentation.Builder sb = SentenceSegmentation.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setMetadata(metadata());
		for(AgigaSentence sentence : doc.getSents())
			sb = sb.addSentence(convertSentence(sentence));
		return sb.build();
	}

	public static SectionSegmentation sectionSegment(AgigaDocument doc, String rawText) {
		return SectionSegmentation.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setMetadata(metadata())
			.addSection(Section.newBuilder()
				.setUuid(IdUtil.generateUUID())
				.setTextSpan(TextSpan.newBuilder()
					.setStart(0)
					.setEnd(rawText.length())
					.build())
				.addSentenceSegmentation(sentenceSegment(doc))
				.build())
			.build();
	}

	public static Communication convertDoc(AgigaDocument doc, KnowledgeGraph kg) {
		CommunicationGUID guid = CommunicationGUID.newBuilder()
			.setCorpusName(corpusName)
			.setCommunicationId(doc.getDocId())
			.build();
		String flatText = flattenText(doc);
		return Communication.newBuilder()
			.setUuid(IdUtil.generateUUID())
			.setGuid(guid)
			.setText(flatText)
			.addSectionSegmentation(sectionSegment(doc, flatText))
			.setKind(Communication.Kind.NEWS)
			.setKnowledgeGraph(kg)
			.build();
	}


	// need some code that reads agiga docs, converts, and then dumps them into a file
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("please provide:");
			System.out.println("1) an input Agiga XML file");
			System.out.println("2) an output Concrete Protobuf file");
			return;
		}
		long start = System.currentTimeMillis();
		File agigaXML = new File(args[0]);	assert(agigaXML.exists() && agigaXML.isFile());
		File output = new File(args[1]);
		StreamingDocumentReader docReader = new StreamingDocumentReader(agigaXML.getPath(), new AgigaPrefs());
		ProtocolBufferWriter writer = new ProtocolBufferWriter(new FileOutputStream(output));

		// TODO we need a knowledge graph
		KnowledgeGraph kg = new ProtoFactory(9001).generateKnowledgeGraph();
		writer.write(kg);

		int c = 0;
		for(AgigaDocument doc : docReader) {
			Communication comm = convertDoc(doc, kg);
			writer.write(comm);
			c++;
		}
		writer.close();
		System.out.printf("done, wrote %d communications to %s in %.1f seconds\n",
			c, output.getPath(), (System.currentTimeMillis() - start)/1000d);
	}
}

