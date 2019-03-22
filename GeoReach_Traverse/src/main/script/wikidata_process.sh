#!/bin/bash
./package.sh
# server
homeDir="/hdd/code/yuhansun"

dataset="wikidata"
# data_dir="${dir}/data/${dataset}"
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

java -Xmx100g -jar ${jar_path} -f wikidataprocess -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 2.0 -MR 2.0