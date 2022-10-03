CONNECTOR_VERSION=$(cat ../version.properties | grep ${connector-version} | cut -d'=' -f2)

# Append option -r to the list of args
args=("-r")
args+=("$@")

spark-submit --master spark://spark:7077 target/scala-2.12/spark-vertica-connector-functional-tests-assembly-$CONNECTOR_VERSION.jar "${args[@]}"
