concrete-agiga
==============

concrete-agiga is a Java library that maps Annotated Gigaword documents to Concrete.

Maven dependency
---
```xml
<dependency>
  <groupId>edu.jhu.hlt</groupId>
  <artifactId>concrete-agiga</artifactId>
  <version>4.2.1</version>
</dependency>
```

## TLDR / Quick start ##
```sh
mvn clean compile assembly:single
java -cp target/concrete-agiga-4.2.1-jar-with-dependencies.jar edu.jhu.hlt.concrete.agiga.AgigaConverter path/to/output/dir drop-annotations path/to/xml/or/xml/gz/file
```

Arguments:
* `path/to/output/dir` - where annotated files will end up
* `drop-annotations` - `boolean` - whether or not to drop annotations that are in the .xml files
  * for RAW files, set to `true`, for ANNOTATED files, set to `false`
* `path/to/xml/or/xml/gz/file` - path to one or more `.xml` or `.xml.gz` files to process

Requirements:
* `java >= 1.7`
* `mvn >= 3.0.4`
* access to COE maven server OR locally installed `concrete-java` library matching this project's version

## Notes ##
One implementation detail to be aware of:
The [anno-pipeline](https://github.com/hltcoe/anno-pipeline) outputs tokens
that contain strings rather than character offsets. So we are not able to
perfectly recreate the original document. The rule this uses is to one space
between tokens and a newline after every sentence. This will only affect you
if you rely on character distances and you use Concrete's TextSpan
(e.g. "Mike's house" => Token("Mike") Token("'s") Token("house") => "Mike 's house"))
