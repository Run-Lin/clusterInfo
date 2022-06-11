#!/usr/bin/env bash
APP_HOME="$(cd "`dirname "$0"`/.."; pwd)"
BUILD_DIR="$APP_HOME/build"
if [ -z "$JAVA_HOME" ]; then
  if [ -z "$JAVA_HOME" ]; then
    if [ $(command -v java) ]; then
      # If java is in /usr/bin/java, we want /usr
      JAVA_HOME="$(dirname $(dirname $(which java)))"
    fi
  fi
fi
if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi
if [ ! "$(command -v mvn)" ] ; then
    echo -e "Specify the Maven command with the --mvn flag"
    exit -1;
fi

cd "$APP_HOME"

BUILD_COMMAND=(mvn -T 1C clean package -DskipTests $@)
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"
"${BUILD_COMMAND[@]}"

# Make directories
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cp "$APP_HOME"/target/*.jar "$BUILD_DIR"/
zip -r "$BUILD_DIR/spark-statis.zip" build bin
rm "$BUILD_DIR"/*.jar
echo "build zip on $BUILD_DIR/spark-statis.zip"
echo -e "Building success ..."




