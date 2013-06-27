concrete-agiga
==============

Tools to map between concrete and agiga representations

To create a JAR with all necessary dependencies:

```$ mvn clean compile assembly:single```

One implementation detail to be aware of:
The [anno-pipeline](https://github.com/hltcoe/anno-pipeline) outputs tokens
that contain strings rather than character offsets. So we are not able to
perfectly recreate the original document. The rule this uses is to one space
between tokens and a newline after every sentence. This will only affect you
if you rely on character distances and you use Concrete's TextSpan 
(e.g. "Mike's house" => Token("Mike") Token("'s") Token("house") => "Mike 's house"))


