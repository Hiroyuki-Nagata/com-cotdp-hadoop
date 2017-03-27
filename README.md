# Hadoop - ZipFileRecordReader/ZipFileInputFormat

Here are a few simple Java classes to simplify working with Hadoop.  The source
is under the Apache License 2.0 and can freely be reused (see LICENSE).

For more information, see my blog - http://cotdp.com/blog/

# Apache Pig - ZipLoader

Using the above Reader/InputFormat, you can dump zip files with Pig

* Dump whole files

```
%declare ZIPLOADER 'com.cotdp.pigudf.ZipLoader';

REGISTER target/com-cotdp-hadoop-1.0-SNAPSHOT.jar

A = LOAD 'src/test/resources/zip-01.zip' USING $ZIPLOADER('');
DUMP A;
```

* Dump with CR or CRLF

```
%declare ZIPLOADER 'com.cotdp.pigudf.ZipLoader';

REGISTER target/com-cotdp-hadoop-1.0-SNAPSHOT.jar

A = LOAD 'src/test/resources/zip-01.zip' USING $ZIPLOADER('\r\n') AS (raw:bytearray);
B = FOREACH A GENERATE FLATTEN($0) AS (raw:chararray);
DUMP B;
```
