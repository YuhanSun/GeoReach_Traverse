#!/bin/bash
./package.sh
# server
homeDir="/hdd/code/yuhansun"

dataset="wikidata"
data_dir="${homeDir}/data/${dataset}"
code_dir="${homeDir}/code"
resultDir="${homeDir}/result"

# local test setup
# dir="/Users/zhouyang/Google_Drive/Projects/tmp/risotree"
# dataset="Yelp"
# data_dir="${dir}/${dataset}"
# code_dir="/Users/zhouyang/Google_Drive/Projects/github_code"

jar_path="${code_dir}/GeoReach_Traverse/GeoReach_Traverse/target/GeoReach_Traverse-0.0.1-SNAPSHOT.jar"
echo "java -Xmx100g -jar ${jar_path} -h"
java -Xmx100g -jar ${jar_path} -h

# java -Xmx100g -jar ${jar_path} -f wikicutLabelFile -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
# java -Xmx100g -jar ${jar_path} -f wikicutPropertyAndEdge -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
# java -Xmx100g -jar ${jar_path} -f wikicutDescription -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0

# java -Xmx100g -jar ${jar_path} -f wikidataExtractProperties -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
# java -Xmx100g -jar ${jar_path} -f wikidataExtractStringLabel -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
# java -Xmx100g -jar ${jar_path} -f extractEntityToEntityRelationEdgeFormat -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0

# java -Xmx100g -jar ${jar_path} -f wikidataLoadGraph -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
# java -Xmx100g -jar ${jar_path} -f wikiLoadEdges -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
java -Xmx100g -jar ${jar_path} -f wikiLoadAttributes -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0

# Not used because entity_properties.txt contains all these information.
# java -Xmx100g -jar ${jar_path} -f wikirecoverSpatialProperty -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
# java -Xmx100g -jar ${jar_path} -f wikirecoverName -hd ${data_dir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0
