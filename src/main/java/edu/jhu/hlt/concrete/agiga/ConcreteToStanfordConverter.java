package edu.jhu.hlt.concrete.agiga;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.DependencyParse;
import edu.jhu.hlt.concrete.Concrete.DependencyParse.Dependency;
import edu.jhu.hlt.concrete.Concrete.Section;
import edu.jhu.hlt.concrete.Concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Concrete.Sentence;
import edu.jhu.hlt.concrete.Concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.Concrete.Token;
import edu.jhu.hlt.concrete.Concrete.TokenTagging;
import edu.jhu.hlt.concrete.Concrete.TokenTagging.TaggedToken;
import edu.jhu.hlt.concrete.Concrete.Tokenization;
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

    // The Stanford TreeGraphNode throws away all but the word from
    // the WordLemmaTag label in converting it to a CoreLabel. Accordingly
    // we allow access to the labels here as well.
    public List<WordLemmaTag> getStanfordWordLemmaTags() {
        Tokenization tokens = sent.getTokenization(tokenizationTheory);
        TokenTagging posTags = tokens.getPosTags(posTagTheory);
        List<WordLemmaTag> labels = new ArrayList<WordLemmaTag>();
        for (int i = 0; i < tokens.getTokenCount(); i++) {
            Token ct = tokens.getToken(i);
            TaggedToken postt = posTags.getTaggedToken(i);
            if (ct.getTokenId() != postt.getTokenId()) {
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
            int dependencyTheory) {
        List<TypedDependency> dependencies = new ArrayList<TypedDependency>();
        if (this.nodes == null) {
            nodes = getStanfordTreeGraphNodes(dependencyTheory);
        }

        DependencyParse depParse = sent.getDependencyParse(dependencyTheory);
        for (Dependency arc : depParse.getDependencyList()) {
            // Add one, since the tokens are zero-indexed but the TreeGraphNodes
            // are one-indexed
            TreeGraphNode gov = nodes.get(arc.getGov().getTokenId() + 1);
            TreeGraphNode dep = nodes.get(arc.getDep().getTokenId() + 1);
            // Create the typed dependency
            TypedDependency typedDep = new TypedDependency(
                    GrammaticalRelation.valueOf(arc.getEdgeType()), gov, dep);
            dependencies.add(typedDep);
        }
        return dependencies;
    }

    public List<TreeGraphNode> getStanfordTreeGraphNodes(int dependencyTheory) {
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

        DependencyParse depParse = sent.getDependencyParse(dependencyTheory);
        for (Dependency arc : depParse.getDependencyList()) {
            // Add one, since the tokens are zero-indexed but the TreeGraphNodes
            // are one-indexed
            TreeGraphNode gov = nodes.get(arc.getGov().getTokenId() + 1);
            TreeGraphNode dep = nodes.get(arc.getDep().getTokenId() + 1);

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
        Communication communication;
        while ((communication = Communication.parseDelimitedFrom(is)) != null) {
            for (SectionSegmentation sectionSegmentation : communication
                    .getSectionSegmentationList()) {
                for (Section section : sectionSegmentation.getSectionList()) {
                    for (SentenceSegmentation sentSegmentation : section
                            .getSentenceSegmentationList()) {
                        for (Sentence sent : sentSegmentation.getSentenceList()) {
                            int i;
                            ConcreteToStanfordConverter scs = new ConcreteToStanfordConverter(
                                    sent);
                            i = 0;
                            for (WordLemmaTag tok : scs
                                    .getStanfordWordLemmaTags()) {
                                if (i++ > 0) {
                                    System.out.print(" ");
                                }
                                System.out.print(tok.word() + "/" + tok.tag());
                            }
                            System.out.println("");
                            i = 0;
                            for (TypedDependency td : scs
                                    .getStanfordTypedDependencies(0)) {
                                if (i++ > 0) {
                                    System.out.print(", ");
                                }
                                System.out.print(td.gov() + "-->" + td.dep()
                                        + "/" + td.reln());
                            }
                            System.out.println("");
                            System.out.println("");
                        }
                    }
                }
            }
        }
        is.close();
    }

}
