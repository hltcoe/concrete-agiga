
echo "COMPILING..."
mvn compile || exit -1

echo ""
echo "RUNNING..."
agigaXML=/home/hltcoe/twolfe/miniScale2013/parma/data/roth_frank/roth_frank_docs.annotated.20130301.xml
output=rf-concrete-dump.pb.gz
cp="/home/hltcoe/twolfe/miniScale2013/parma/lib/*:target/classes/"
time java -classpath $cp edu.jhu.concrete.agiga.AgigaConverter $agigaXML $output


