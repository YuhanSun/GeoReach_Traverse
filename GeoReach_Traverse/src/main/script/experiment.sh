#!/bin/bash

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

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
echo "java -Xmx100g -jar ${jar_path} -h"
java -Xmx100g -jar ${jar_path} -h

# run the query
echo "java -Xmx100g -jar ${jar_path} -f inser -dp ${db_path} -d ${dataset} -gp ${graph_path} -ep ${entity_path} -lp ${label_path}"
# create the db first
java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -testRatio 0.5 -us LIGHTWEIGHT
# run the query
java -Xmx100g -jar ${jar_path} -f query -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -us LIGHTWEIGHT -ex SPATRAVERSAL


# java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -testRatio 0.5 -e LIGHTWEIGHT
# java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -testRatio 0.5 -e LIGHTWEIGHT
# java -Xmx100g -jar ${jar_path} -f insertion -hd ${homeDir} -rd ${resultDir} -d ${dataset} -MG 1.0 -MR 2.0 -testRatio 0.5 -e LIGHTWEIGHT