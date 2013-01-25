hio\_bench
======================
This tests random and sequence HDFS I/O.

Building
-------------------------------------------------------------
In order to build and run this test, you must put the HDFS and Hadoop-common
jar files into your classpath.  In the Hadoop install, these are found under
share/hadoop/common/ share/hadoop/hdfs/

TODO: use Ivy.

Running
-------------------------------------------------------------
You have to set some Java system properties when running the test.

Here is an example of how to run the test:

    ANT\_OPTS="-Dhio.nthreads=5 -Dhio.ngigs.to.read=10 -Dhio.ngigs.in.file=10 -Dhio.hdfs.uri=hdfs://localhost:4000" ant compile jar run

It's also important to make sure libhadoop.so is in your LD\_LIBRARY\_PATH;
otherwise, you won't get features like short-circuit local reads which are
important for performance.

Alternately, you can directly run the jar with:

    java -Dhio.nthreads=5 -Dhio.ngigs.to.read=1 -Dhio.ngigs.in.file=1 -Dhio.hdfs.uri=hdfs://localhost:6000 com.cloudera.HioBench

Contact information
-------------------------------------------------------------
Colin Patrick McCabe <cmccabe@alumni.cmu.edu>
