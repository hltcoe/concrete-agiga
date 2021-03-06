package edu.jhu.hlt.concrete.agiga;

import java.io.IOException;
import java.util.List;

import concrete.tools.AnnotationException;
import edu.jhu.agiga.AgigaDocument;
import edu.jhu.agiga.AgigaSentence;
import edu.jhu.agiga.AgigaToken;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.UUID;
import edu.jhu.hlt.concrete.util.ConcreteUUIDFactory;
import edu.jhu.hlt.concrete.util.TokenizationUtils.TagTypes;
import edu.stanford.nlp.trees.Tree;

/**
 * Adds annotations from Agiga objects to Concrete objects using the AgigaConverter where possible.
 * 
 * @author mgormley
 */
public class AgigaAnnotationAdder {

    private static final ConcreteUUIDFactory idF = new ConcreteUUIDFactory();

    public static void addAgigaAnnosToSection(AgigaDocument aDoc, Section cSection) throws IOException {
        List<Sentence> cSents = cSection.getSentenceList();
        List<AgigaSentence> aSents = aDoc.getSents();
        if (cSents.size() != aSents.size()) {
            throw new IllegalStateException("Mismatch between number of sentences: " + cSents.size() + " " + aSents.size());
        }
        for (int i=0; i<cSents.size(); i++) {
            Sentence cSent = cSents.get(i);
            AgigaSentence aSent = aSents.get(i);
            AgigaAnnotationAdder.addAgigaAnnosToConcreteSent(aSent, cSent);
        }
    }

    /**
     * Add the annotations found in an {@link AgigaSentence} to an existing Concrete {@link Sentence}.
     * @throws IOException 
     */
    public static void addAgigaAnnosToConcreteSent(AgigaSentence aSent, Sentence cSent) throws IOException {
        Tokenization cTokenization = cSent.getTokenization();
        addAgigaAnnosToConcreteTokenization(aSent, cTokenization);
    }

    /**
     * Add the annotations found in an {@link AgigaSentence} to an existing Concrete {@link Tokenization}.
     * @throws IOException 
     */
    public static void addAgigaAnnosToConcreteTokenization(AgigaSentence aSent, Tokenization cTokenization) throws IOException {
        checkMatchingTokenizations(aSent, cTokenization);
        AgigaConverter converter = new AgigaConverter(false);
        UUID tUuid = cTokenization.getUuid();
        TokenTagging lemma = new TokenTagging();
        lemma.setUuid(idF.getConcreteUUID());
        lemma.setMetadata(converter.getLemmaMetadata(tUuid));
        lemma.setTaggingType(TagTypes.LEMMA.name());
        
        TokenTagging pos = new TokenTagging();
        pos.setUuid(idF.getConcreteUUID());
        pos.setMetadata(converter.getPOSMetadata(tUuid));
        lemma.setTaggingType(TagTypes.POS.name());
        
        TokenTagging ner = new TokenTagging();
        ner.setUuid(idF.getConcreteUUID());
        ner.setMetadata(converter.getNERMetadata(tUuid));
        lemma.setTaggingType(TagTypes.NER.name());
        
        int n = aSent.getTokens().size();
        for (int tokId = 0; tokId < n; tokId++) {
          AgigaToken tok = aSent.getTokens().get(tokId);

          // token annotations
          lemma.addToTaggedTokenList(converter.makeTaggedToken(tok.getLemma(), tokId));
          pos.addToTaggedTokenList(converter.makeTaggedToken(tok.getPosTag(), tokId));
          ner.addToTaggedTokenList(converter.makeTaggedToken(tok.getNerTag(), tokId));
        }

        cTokenization.addToTokenTaggingList(lemma);
        cTokenization.addToTokenTaggingList(pos);
        cTokenization.addToTokenTaggingList(ner);

        UUID tzUUID = cTokenization.getUuid();
        try {
          Tree root = aSent.getStanfordContituencyTree();
          List<Tree> leaves = root.getLeaves();
          if (leaves.size() != n) {
            int i = 0;
            for (Tree node : leaves)
              System.out.println((i++) + ": " + node.toString());
            for (i = 0; i < n; i++)
              System.out.println(i + ": " + aSent.getTokens().get(i).getWord());
            throw new AnnotationException("number of leaves in the parse (" + leaves.size()
                + ") is not equal to the number of tokens in the sentence (" + n+ ")");
          }
          cTokenization.addToParseList(converter.stanford2concrete(root, n, tzUUID));
        } catch (AnnotationException ae) {
          throw new RuntimeException("Unsure how to proceed given exception.", ae);
        }
        cTokenization.addToDependencyParseList(converter.convertDependencyParse(aSent.getBasicDeps(), "basic-deps", tzUUID));
        cTokenization.addToDependencyParseList(converter.convertDependencyParse(aSent.getColDeps(), "col-deps", tzUUID));
        cTokenization.addToDependencyParseList(converter.convertDependencyParse(aSent.getColCcprocDeps(), "col-ccproc-deps", tzUUID));
    }

    /** Check that the {@link AgigaSentence} corresponds to the given Concrete {@link Tokenization}. */
    private static void checkMatchingTokenizations(AgigaSentence aSent, Tokenization cTokenization) {
        List<AgigaToken> aToks = aSent.getTokens();
        List<Token> cToks = cTokenization.getTokenList().getTokenList();
        if (aToks.size() != cToks.size()) {
            throw new IllegalStateException("Number of tokens not equal: agiga=" + aToks.size()
                    + " concrete=" + cToks.size());
        }
        for (int i=0; i<cToks.size(); i++) {
            AgigaToken aTok = aToks.get(i);
            Token cTok = cToks.get(i);
            if (!aTok.getWord().equals(cTok.getText())) {

                throw new IllegalStateException("Token text not equal: agiga=" + aTok.getWord()
                        + " concrete=" + cTok.getText());
            }
        }
    }
}
