package edu.jhu.hlt.concrete.agiga;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFileTransport;

import edu.jhu.hlt.concrete.Dependency;
import edu.jhu.hlt.concrete.DependencyParse;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.TaggedToken;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenTagging;
import edu.jhu.hlt.concrete.Tokenization;
import edu.stanford.nlp.ling.Label;
import edu.stanford.nlp.ling.WordLemmaTag;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TreeGraphNode;
import edu.stanford.nlp.trees.TypedDependency;

/**
 * Converter of a Concrete sentence to the Stanford API objects.
 * 
 * @author mgormley
 */
public class ConcreteToStanfordConverter {

    public static final long serialVersionUID = 1;

    private static final Label ROOT_LABEL = new WordLemmaTag("ROOT");

    private List<TreeGraphNode> nodes = null;
    private Sentence sent;

    private int tokenizationTheory;
    private int posTagTheory;
    private static final int DEFAULT_THEORY = 0;

    /**
     * Constructs a StanfordConcreteSentence which uses the 0'th tokenization
     * and POS tag theories.
     * 
     * @param sent The Concrete sentence.
     */
    public ConcreteToStanfordConverter(Sentence sent) {
        this(sent, DEFAULT_THEORY, DEFAULT_THEORY);
    }

    /**
     * Constructs a StanfordConcreteSentence.
     * 
     * @param sent The Concrete sentence.
     * @param tokenizationTheory The theory for the Tokenization.
     * @param posTagTheory The theory for the POS tagging.
     */
    public ConcreteToStanfordConverter(Sentence sent, int tokenizationTheory,
            int posTagTheory) {
        this.sent = sent;
        this.tokenizationTheory = tokenizationTheory;
        this.posTagTheory = posTagTheory;
    }
    
    public List<TokenTagging> getPOSTags(Tokenization t) {
      List<TokenTagging> toRet = new ArrayList<TokenTagging>();
      List<TokenTagging> ttList = t.getTokenTaggingList();
      for (TokenTagging tt : ttList) {
        if (tt.getTaggingType().toLowerCase().equals("pos"))
          toRet.add(tt);
      }
      
      return toRet;
    }

    // The Stanford TreeGraphNode throws away all but the word from
    // the WordLemmaTag label in converting it to a CoreLabel. Accordingly
    // we allow access to the labels here as well.
    public List<WordLemmaTag> getStanfordWordLemmaTags() {
        Tokenization tokens = sent.getTokenizationList().get(0);
        // Assumes one POS tag, or will only get the first found. 
        TokenTagging posTags = this.getPOSTags(tokens).get(0);
        List<WordLemmaTag> labels = new ArrayList<WordLemmaTag>();
        List<Token> tokenList = tokens.getTokenList().getTokenList();
        List<TaggedToken> ttList = posTags.getTaggedTokenList();
        for (int i = 0; i < tokens.getTokenList().getTokenListSize(); i++) {
            Token ct = tokenList.get(i);
            TaggedToken postt = ttList.get(i);
            if (ct.getTokenIndex() != postt.getTokenIndex()) {
                throw new IllegalStateException("Expected token ids to match");
            }
            WordLemmaTag curToken;
            // TODO: Where would the lemma live? curToken = new
            // WordLemmaTag(ct.getText(), ct.getLemma(), ct.getPosTag());
            curToken = new WordLemmaTag(ct.getText(), postt.getTag());
            labels.add(curToken);
        }
        return labels;
    }

    public List<TypedDependency> getStanfordTypedDependencies(
			int tokenizationTheory,
            int dependencyTheory) {
        List<TypedDependency> dependencies = new ArrayList<TypedDependency>();
        if (this.nodes == null) {
            nodes = getStanfordTreeGraphNodes(tokenizationTheory, dependencyTheory);
        }

        Tokenization tok = sent.getTokenizationList().get(0);
        DependencyParse depParse = tok.getDependencyParseList().get(dependencyTheory);
        for (Dependency arc : depParse.getDependencyList()) {
            // Add one, since the tokens are zero-indexed but the TreeGraphNodes
            // are one-indexed
            TreeGraphNode gov = nodes.get(arc.getGov() + 1);
            TreeGraphNode dep = nodes.get(arc.getDep() + 1);
            // Create the typed dependency
            TypedDependency typedDep = new TypedDependency(
                    GrammaticalRelation.valueOf(arc.getEdgeType()), gov, dep);
            dependencies.add(typedDep);
        }
        return dependencies;
    }

    public List<TreeGraphNode> getStanfordTreeGraphNodes(
			int tokenizationTheory,
			int dependencyTheory) {
        if (this.nodes != null)
            return this.nodes;

        this.nodes = new ArrayList<TreeGraphNode>();
        // Add an explicit root node
        nodes.add(new TreeGraphNode(ROOT_LABEL));

        List<WordLemmaTag> labels = getStanfordWordLemmaTags();
        for (WordLemmaTag curToken : labels) {
            // Create the tree node
            TreeGraphNode treeNode = new TreeGraphNode(curToken);
            treeNode.label().setTag(curToken.tag());
            /**
             * Caution, the order to call is to first setWord(), then
             * setlemma().
             */
            treeNode.label().setWord(curToken.word());
            treeNode.label().setLemma(curToken.lemma());
            nodes.add(treeNode);
        }

        Tokenization tok = sent.getTokenizationList().get(0);
        DependencyParse depParse = tok.getDependencyParseList().get(dependencyTheory);
        for (Dependency arc : depParse.getDependencyList()) {
            // Add one, since the tokens are zero-indexed but the TreeGraphNodes
            // are one-indexed
            TreeGraphNode gov = nodes.get(arc.getGov() + 1);
            TreeGraphNode dep = nodes.get(arc.getDep() + 1);

            // Add gov/dep to TreeGraph
            gov.addChild(dep);
            dep.setParent(gov);
            if (dep.parent() != gov) {
                throw new IllegalStateException("Invalid parent for dep");
            }
        }

        return nodes;
    }

    /**
     * Reads a Protocol Buffer file containing Commmunications, converts the
     * dependency trees to Stanford objects, and prints them out in a human
     * readable form.
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("usage: java "
                    + ConcreteToStanfordConverter.class + " <input file>");
            System.exit(1);
        }
        File inputFile = new File(args[0]);
        if (!inputFile.exists()) {
            System.err.println("ERROR: File does not exist: " + inputFile);
            System.exit(1);
        }

        InputStream is = new FileInputStream(inputFile);
        if (inputFile.getName().endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        TFileTransport tf = new TFileTransport(args[0], true);
//        
//        Communication communication;
//        while ((communication = Communication.parseDelimitedFrom(is)) != null) {
//            for (SectionSegmentation sectionSegmentation : communication.getSectionSegmentationList()) {
//                for (Section section : sectionSegmentation.getSectionList()) {
//                    for (SentenceSegmentation sentSegmentation : section.getSentenceSegmentationList()) {
//                        for (Sentence sent : sentSegmentation.getSentenceList()) {
//                            int i;
//                            ConcreteToStanfordConverter scs = new ConcreteToStanfordConverter(sent);
//                            i = 0;
//                            for (WordLemmaTag tok : scs.getStanfordWordLemmaTags()) {
//                                if (i++ > 0) {
//                                    System.out.print(" ");
//                                }
//                                System.out.print(tok.word() + "/" + tok.tag());
//                            }
//                            System.out.println("");
//                            i = 0;
//                            for (TypedDependency td : scs.getStanfordTypedDependencies(0, 0)) {
//                                if (i++ > 0) {
//                                    System.out.print(", ");
//                                }
//                                System.out.print(td.gov() + "-->" + td.dep()
//                                        + "/" + td.reln());
//                            }
//                            System.out.println("");
//                            System.out.println("");
//                        }
//                    }
//                }
//            }
//        }
        is.close();
    }

}
