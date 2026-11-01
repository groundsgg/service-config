# Config native pilot

All runs use disposable PostgreSQL, PgBouncer, two connected NATS brokers, an HTTPS
JWKS issuer, and two instances of the selected application image. They never use the
Stage database. `images.json` records resolved upstream digests.

## Build and verify

Use Java 25, Docker/Buildx, Python 3.12+, OpenSSL and the existing Gradle package
credentials. `PACKAGES_TOKEN` must have access to Grounds Maven packages. Do not
put it into an image or a committed file.

```sh
./gradlew --no-daemon test quarkusBuild
BUILD_COMMIT=$(git rev-parse HEAD)
BUILD_SOURCE_SHA256=$(python3 benchmarks/native-pilot/provenance.py --source-hash)
docker build -f benchmarks/native-pilot/Dockerfile.jvm \
  --build-arg BUILD_COMMIT="$BUILD_COMMIT" --build-arg BUILD_SOURCE_SHA256="$BUILD_SOURCE_SHA256" \
  -t grounds/service-config:jvm-pilot build
./gradlew --no-daemon runtimeTest -Dgrounds.runtime.image=grounds/service-config:jvm-pilot

docker buildx build --load -f Dockerfile.native --target native-micro \
  --build-arg BUILD_COMMIT="$BUILD_COMMIT" --build-arg BUILD_SOURCE_SHA256="$BUILD_SOURCE_SHA256" \
  --build-arg GITHUB_USER="$GITHUB_USER" --secret id=github_token,env=PACKAGES_TOKEN \
  -t grounds/service-config:native-micro-pilot .
docker buildx build --load -f Dockerfile.native --target native-distroless \
  --build-arg BUILD_COMMIT="$BUILD_COMMIT" --build-arg BUILD_SOURCE_SHA256="$BUILD_SOURCE_SHA256" \
  --build-arg GITHUB_USER="$GITHUB_USER" --secret id=github_token,env=PACKAGES_TOKEN \
  -t grounds/service-config:native-distroless-pilot .
python3 benchmarks/native-pilot/verify-images.py \
  grounds/service-config:native-micro-pilot grounds/service-config:native-distroless-pilot
./gradlew --no-daemon runtimeTest -Dgrounds.runtime.image=grounds/service-config:native-micro-pilot
./gradlew --no-daemon runtimeTest -Dgrounds.runtime.image=grounds/service-config:native-distroless-pilot
python3 -m unittest discover -s benchmarks/native-pilot -p 'test_*.py'
```

The driver rejects images whose build revision/source labels differ from the
checkout and records the resolved image IDs. `matrix.py` freezes those IDs for the
entire comparison and rejects retagged images. Native binary bytes are hashed again
at measurement time rather than trusting only the packaged checksum.
Native builds initialize jnats random generators at process start; the
target architecture is the portable amd64 baseline. Distroless must pass the same
tests as Micro before it is eligible. If it fails, record the reason and exclude it
from the release comparison; Micro remains eligible after its own gates pass.

## Run the comparison

Commit the candidate before measurement and leave its source unchanged across all
runs. Keep application resources at 1 CPU and 768 MiB. Run no concurrent builds.
The host-side load driver has separate resources from the CPU-limited containers.
The local comparison has **no Istio sidecar** and has OTel disabled for every
variant; it cannot replace the later Stage validation with identical telemetry and
mesh settings. JVM flags are configured only on the tuned variant.

Choose an absolute output directory outside the source checkout. One invocation
runs one repetition (35 minutes plus fixture setup). The full 4-variant, 3-repeat
matrix takes roughly 7 hours. The initial proposed 500/25 burst overloaded the JVM
baseline; the common calibrated burst is 200 reads and 10 writes per second.
Rotate variant order between repetitions, or run the complete serial matrix:

```sh
python3 benchmarks/native-pilot/matrix.py --output /tmp/config-native-full
```

```sh
python3 benchmarks/native-pilot/run.py --variant jvm \
  --image grounds/service-config:jvm-pilot --repetition 0 --output /tmp/config-native-results
python3 benchmarks/native-pilot/run.py --variant jvm-tuned \
  --image grounds/service-config:jvm-pilot --repetition 0 --output /tmp/config-native-results
python3 benchmarks/native-pilot/run.py --variant native-micro \
  --image grounds/service-config:native-micro-pilot --repetition 0 --output /tmp/config-native-results
python3 benchmarks/native-pilot/run.py --variant native-distroless \
  --image grounds/service-config:native-distroless-pilot --repetition 0 --output /tmp/config-native-results
# Repeat for repetitions 1 and 2, rotating the order.
python3 benchmarks/native-pilot/summarize.py --input /tmp/config-native-results \
  --output /tmp/config-native-results/summary.json
```

Request latency starts at scheduled arrival, including driver dispatch delay.
Only successful completions within the phase deadline count toward its achieved
rate. Late drained requests remain in the latency/error evidence; regular and burst
per-operation latency gates are both checked. CPU and working-set integration is
clipped to the phase duration.

`manifest.json` records source, scenario and context hashes, image ID, cold start
to readiness, uncompressed local image bytes, and restart/OOM state. Local image
bytes are **not** compressed registry transfer bytes. Native binary checksums are
also recorded. `raw.jsonl` contains requests and cgroup working-set/anonymous RSS
and CPU samples. Image transfer sizes and CI peak build memory are recorded
separately from these runtime metrics.

Readiness failure can be HTTP 503 or a probe timeout on the existing JVM; both
remove readiness. Recovery must still return HTTP 200. JSON document comparisons
are semantic because PostgreSQL jsonb normalizes whitespace.

The driver uses a private mode-0600 fixture bridge containing disposable test
credentials. It removes that bridge on shutdown. Partial measurements and crashed
drivers fail the decision; missing repetitions are never presented as a pass.
Pressing Ctrl-C requests teardown of the fixture. Only containers created by that
fixture are removed. Large fixture logs and measurement output belong in artifacts,
not the repository.
