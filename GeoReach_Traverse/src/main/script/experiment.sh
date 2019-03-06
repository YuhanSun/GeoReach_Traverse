#!/bin/bash

# server
dir="/hdd/code/yuhansun"
dataset="Yelp"
data_dir="${dir}/data/${dataset}"
code_dir="${dir}/code"

# local test setup
# dir="/Users/zhouyang/Google_Drive/Projects/tmp/risotree"
# dataset="Yelp"
# data_dir="${dir}/${dataset}"
# code_dir="/Users/zhouyang/Google_Drive/Projects/github_code"

jar_path="${code_dir}/Riso-Tree/target/Riso-Tree-0.0.1-SNAPSHOT.jar"
echo "java -Xmx100g -jar ${jar_path} -h"
java -Xmx100g -jar ${jar_path} -h

echo "java -Xmx100g -jar ${jar_path} -f tree -dp ${db_path} -d ${dataset} -gp ${graph_path} -ep ${entity_path} -lp ${label_path}"
java -Xmx100g -jar ${jar_path} -f tree -dp ${db_path} -d ${dataset} -gp ${graph_path} -ep ${entity_path} -lp ${label_path}