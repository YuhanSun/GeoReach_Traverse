#!/bin/bash
./package.sh
# server
homeDir="/hdd/code/yuhansun"
dataset="Yelp"
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

# run the query
# create the db first
# java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -testRatio 0.5 -us LIGHTWEIGHT
java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG -1.0 -MR 2.0 -testRatio 0.5 -us LIGHTWEIGHT
java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG -1.0 -MR -1.0 -testRatio 0.5 -us LIGHTWEIGHT

# run the query
# java -Xmx100g -jar ${jar_path} -f query -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -us LIGHTWEIGHT -ex SPATRAVERSAL

