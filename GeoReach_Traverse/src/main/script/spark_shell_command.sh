# /hdd2/code/spark-2.3.2-bin-hadoop2.6/sbin/start-all.sh

cd /hdd2/code/spark-2.3.2-bin-hadoop2.6/
./bin/spark-shell --master local[*]
val df = spark.read.format("csv").option("header","false").option("delimiter"," ").load("/hdd/code/yuhansun/data/wikidata/wikidata-20180308-truthy-BETA.nt");


df.createOrReplaceTempView("motherTable");

val tb = spark.sql("select * from motherTable where _c3 LIKE '%@en%'");

tb.createOrReplaceTempView("tb");
val labelDf = spark.sql("select * from tb where _c1 LIKE '%label%' OR _c1 LIKE '%Label%'");
labelDf.write.format("csv").option("header","false").option("delimiter"," ").save("/hdd/code/yuhansun/data/wikidata/labelDf")