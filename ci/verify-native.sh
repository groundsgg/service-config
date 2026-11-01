#!/usr/bin/env bash
set -euo pipefail

./gradlew --no-daemon -Pkotlin.compiler.execution.strategy=in-process quarkusBuild
docker build -f src/runtimeTest/docker/Dockerfile.jvm \
  --build-arg BUILD_COMMIT="$GITHUB_SHA" --build-arg BUILD_SOURCE_SHA256="$BUILD_SOURCE_SHA256" \
  -t "$NATIVE_IMAGE_PREFIX:jvm-pilot" build
for runtime in jvm $NATIVE_TARGETS; do
  ./gradlew --no-daemon -Pkotlin.compiler.execution.strategy=in-process runtimeTest \
    -Dgrounds.runtime.image="$NATIVE_IMAGE_PREFIX:$runtime-pilot"
done
