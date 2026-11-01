# Config Native Memory Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Umsetzung durch den primären Agenten; separate kritische Abschlussprüfung.

**Goal:** An `service-config` messen, ob Native Image den RAM-Bedarf bei unveränderter HTTP-, Auth- und Netzwerkfunktion ausreichend reduziert.

**Architecture:** Parallel nutzbare JVM- und Native-Images desselben Commits; identisches Native-Binary auf Quarkus Micro und Google Distroless. Erst isolierter Vergleich, dann ein regional begrenzter Stage-Kandidat mit unverändertem JVM-Digest als Rollback.

**Tech Stack:** Java 25, Kotlin, Quarkus 3.40.1, Gradle, Mandrel/GraalVM, Docker BuildKit, GitHub Actions/GHCR, PostgreSQL/PgBouncer, NATS, Kubernetes/Istio, Prometheus/Mimir.

**Spec:** `../specs/2026-11-01-native-memory-pilot-design.md` — Designentwurf mit vorgeschlagenen Abnahmegrenzen. Dieser Plan ist zur Durchsicht; keine Implementierung oder Aktivierung ist erfolgt.

## Global Constraints

- Ausgangspunkt: verifizierter `service-config` v1.5.0-Commit `7ac9de91ce025a33c3afd9efdaa3ee0450c6257f`; Kandidatenvergleich danach auf identischem Commit.
- Java 25, Quarkus 3.40.1 und Linux amd64; Builder und Runtime per Digest pinnen. Toolchain-Kompatibilität verifizieren, kein beiläufiges Framework-Upgrade.
- Öffentlicher Transport ist HTTP/JSON. Die gRPC-Erweiterung dient noch der Generierung interner Nachrichtentypen.
- Alle Grounds-Funktionen müssen proxyübergreifend funktionieren; keine lokale Proxy-Presence als Netzwerk-Wahrheit verwenden.
- Keine Schemaänderung, kein Umbau von Auth oder Event-Delivery; PostgreSQL bleibt autoritativ, NATS-Hinweise bleiben best-effort.
- Bestehender JVM-Build bleibt verfügbar; Native bekommt eigene immutable Tags. Keine Builds auf dem Game-Node.
- Kein Reset, Stash oder Überschreiben fremder Änderungen. Die lokale ungetrackte `service-config/grounds.yaml` blockiert derzeit den Pull.
- Dateipfade unten beziehen sich auf `/home/lukas/grounds/service-config`, sofern ein anderes Repository nicht genannt ist. Tatsächliche aktuelle Pfade vor Änderungen überprüfen.

## Review Focus

1. Native-JSON und Ressourcen: HTTP-DTOs, interne Protobuf-Typen, SQL-Migrationen und Fehlerantworten dürfen nicht erst in seltenen Pfaden fehlen — Task 2.
2. Vertrauenskette und Tokenwechsel: HTTPS-JWKS, ungültiges JWT, rotierter Schlüssel und erneuerter NATS-Token müssen die gleichen Entscheidungen liefern — Task 2.
3. Verpasstes Event und entfernter Proxy: Polling und zwei unabhängige Consumer müssen zum gleichen Snapshot gelangen — Task 2 und 5.
4. Pooler und paralleler Start: Flyway direkt, Runtime über PgBouncer; keine neue Schemaänderung oder Migration auf jeder Fehlerwiederholung — Task 2 und 5.
5. Messverzerrung: identischer Commit/Datensatz/Telemetrie/Sidecar und wirklich erreichte Last; getrennte App-/Sidecar-Werte — Task 4.

## Task 1: Verifizierte Arbeitsbasis und wiederholbarer JVM-Vergleich

**Files:** vorhandene `Dockerfile`, `build.gradle.kts`, `gradle/libs.versions.toml`, `src/main/resources/application.properties`; neu `benchmarks/native-pilot/scenarios.json` und `benchmarks/native-pilot/README.md`.

**Interfaces:** Liefert `scenarios.json` mit `schemaVersion: 1`, Datensatz (100 Dokumente, 4096 Byte JSON), drei Wiederholungen und den Phasen/Abnahmegrenzen aus der Spec. Liefert im Messmanifest Commit, JVM-Image-Digest, Toolchain und Ressourcen.

- [ ] Lokalen Git-Zustand erneut prüfen. In einem geeigneten sauberen Checkout den verifizierten Commit bereitstellen, ohne `grounds.yaml` oder andere Benutzeränderungen anzutasten. Native-Implementierung auf einem eigenen Branch beginnen.
- [ ] Provenienz des eingesetzten JVM-Images prüfen und den alten Digest für Rollback festhalten. Aktuelles main nur nach dokumentiertem Vergleich zum verifizierten Commit als Ausgangspunkt wählen.
- [ ] Aktuelle Build-Konfiguration und shared Workflow `groundsgg/.github/.github/workflows/docker-gradle-build-push.yml` lesen. Ein- und Ausgaben einschließlich BuildKit-Secrets beibehalten.
- [ ] Bestehende Tests ausführen: `./gradlew --no-daemon test`. Keine Zugangsdaten in Logs, Build-Args oder Artefakten ablegen.
- [ ] Scheitert ein neuer Runtime-Vertragstest bereits gegen die unveränderte JVM, bestehenden Fehler separat dokumentieren. Auth-/Konsistenz-/Konvergenzfehler sperren die Stage-Aktivierung; keine stillen Funktionskorrekturen mit der Runtime-Migration vermischen.
- [ ] Szenario und Messmanifest definieren. Die zwei JVM-Varianten bauen aus demselben späteren Kandidaten-Commit: Standard und `JAVA_TOOL_OPTIONS=-Xms16m -Xmx128m`. Gemeinsames App-Limit zunächst 768 MiB; den aktuell tatsächlich wirksamen CPU-Rahmen als identischen Vergleichsrahmen übernehmen.
- [ ] Falls die optimierte JVM unter dem Szenario scheitert, den Heap auf 192 MiB anheben, Entscheidung protokollieren und alle betroffenen Vergleichsläufe wiederholen. Keine Flags auf Basis der Idle-Messung produktiv ausrollen.

**Done:** JVM-Build und Tests reproduzierbar; Ausgangs-Commit, Bild-Digest, Ressourcen und Szenario maschinenlesbar gespeichert.

## Task 2: Echte Runtime-Verträge und Native-Kompatibilität nachweisen

**Files:** ändern `build.gradle.kts`; neu `src/runtimeTest/kotlin/gg/grounds/RuntimeFixture.kt`, `ConfigRuntimeContractTest.kt`, `ConfigNetworkDeliveryTest.kt`; bei nachgewiesenem Bedarf konkrete DTO-/Ressourcenregistrierungen in `src/main`. Bestehende Tests in `src/test/kotlin/gg/grounds/rest` und `auth` weiterverwenden.

**Interfaces:** Gradle-Testtask `runtimeTest` verwendet ein separates Source Set ohne CDI-/Mockito-Mocks. Systemproperty `grounds.runtime.image` bestimmt das fertig gebaute Image. `RuntimeFixture.start(image: String)` startet isoliert Datenbank, Pooler, NATS und HTTPS-JWKS, liefert Basis-URL, Test-Token und unabhängige Consumer; `close()` räumt ausschließlich die erzeugten Testressourcen auf.

- [ ] Testcontainers-/JUnit-Abhängigkeiten passend zum vorhandenen Versionsmanagement ergänzen. Fixtures pinnen ihre Container; JWKS-Zertifikat wird von einer Test-CA ausgestellt und im Anwendungscontainer vertraut.
- [ ] Erst die echten HTTP-Tests schreiben und gegen das JVM-Image ausführen: Dokument erzeugen/lesen/ersetzen/löschen, Defaults idempotent, Snapshot `200` und bei unverändertem ETag `304`, korrektes Dokument-ETag, veraltetes `If-Match` ergibt `409`, ungültiges JSON/Versionen liefern den bestehenden Fehlervertrag.
- [ ] Auth-Tests: gültiger Reader, erlaubter scoped Writer, fremde App, fehlendes/abgelaufenes Token, falsche Audience, JWKS-Keyrotation und TLS-Vertrauen. HTTP-Status/Problem-Body mit dem aktuellen JVM-Vertrag vergleichen, keine Auth-Abschaltung für Native.
- [ ] Delivery-Tests: zwei Consumer mit unterschiedlichen Proxy-Identitäten; Schreiben über Instanz A, Lesen über Instanz B. Event-JSON/Subject prüfen. Einen Consumer beim Publish trennen und anschließend Konvergenz über Snapshot-Polling nachweisen. Dieselbe Suite mit zwei über Leaf/Hub verbundenen NATS-Brokern ausführen.
- [ ] Fehlerfälle isoliert auslösen: NATS beim Write nicht erreichbar → gespeicherter Wert bleibt korrekt lesbar; Broker-Neustart und erneuerter Token → Verbindung erholt sich. DB-Ausfall → Readiness DOWN; NATS-Ausfall allein → bestehende DB-basierte Readiness-Semantik bleibt erhalten.
- [ ] Runtime-Queries über PgBouncer und Migrationen direkt testen, auch bei zwei gleichzeitig startenden Instanzen. Dafür eine eigene temporäre Datenbank verwenden; niemals Stage-Dokumente ändern.
- [ ] Gegen JVM laufen lassen: `./gradlew runtimeTest -Dgrounds.runtime.image=<jvm-image>`. Dieselben Assertions später unverändert gegen beide Native-Runtime-Basen ausführen.

**Done:** Blackbox-Suite prüft tatsächliche HTTP-/TLS-/DB-/NATS-Pfade. Native-spezifische Fehler führen zu kleinen, durch den Fehler begründeten Registrierungen, nicht zu pauschalen Reflection-Freigaben.

## Task 3: Native-Build und zwei minimale Runtime-Images

**Files:** neu `Dockerfile.native`, `.github/workflows/native-pilot.yml`; ändern `build.gradle.kts` und `src/main/resources/application.properties` nur bei Bedarf. Bestehende `Dockerfile` und JVM-Workflow bleiben verfügbar.

**Interfaces:** Native-Build liefert `build/*-runner`; Multi-Stage-Dockerfile exportiert `native-micro` und `native-distroless` aus exakt demselben Binary-Stage. Images enthalten denselben Binary-SHA256, Commit und Build-Metadaten.

- [ ] Builder für Java 25/Quarkus 3.40.1 auswählen, Version/ABI/Digest protokollieren. Tatsächliche dynamische Abhängigkeiten des Binaries in einer Diagnose-Stage prüfen.
- [ ] Native mit `./gradlew --no-daemon quarkusBuild -Dquarkus.native.enabled=true -Dquarkus.package.jar.enabled=false` im passenden Builder erzeugen. Bestehendes Uber-JAR bleibt außerhalb dieses Builds unverändert. Native-Laufzeit erhält zunächst explizit `-Xmx192m` im ausführbaren Entrypoint; JVM-Env-Flags werden dafür nicht vorausgesetzt.
- [ ] Micro und `gcr.io/distroless/cc-debian13:nonroot` jeweils per Digest als Runtime testen. Nonroot-User, ausführbares Binary, JSON-Entrypoint, Port 9000, CA-Dateien und Token-Mounts verifizieren. Keine Java-Runtime in native Images kopieren.
- [ ] Auf beiden Images die Task-2-Suite ausführen. Native-/Distroless-spezifische Fehler zuerst reproduzieren und gezielt beheben. Falls Distroless eine fragile Bibliotheksmischung erfordert, den Vergleichskandidaten mit Ursache verwerfen; alle Tests müssen auf der gewählten Micro-Basis bestehen.
- [ ] Separater Workflow baut bei PR die gewählten Runtime-Ziele ohne Push; bei main/manuellem Dispatch publiziert er jedes Ziel erst nach seinem bestandenen Runtime-Test als `native-micro-sha-<commit>` beziehungsweise `native-distroless-sha-<commit>` mit Digest. Gewählter Kandidat bekommt zusätzlich `native-sha-<commit>`. Ein verworfenes Distroless-Ziel bleibt außerhalb der Freigabematrix; JVM-Tags nicht überschreiben.
- [ ] BuildKit-Secrets für GitHub Packages beibehalten; SBOM/Provenienz und Build-/Binary-Digests erfassen. Runner-RAM/Timeout nach einem echten Build festhalten, bevor der Job Standard wird.

**Done:** Zwei getestete Runtime-Images desselben nativen Binaries oder dokumentiert gewählte Micro-Basis; JVM-Rollback bleibt baubar und abrufbar.

## Task 4: RAM, CPU, Latenz und Image-Größe fair vergleichen

**Files:** neu `benchmarks/native-pilot/run.py`, `summarize.py`, `test_summarize.py`; Szenario aus Task 1. Ergebnisse nach `/home/lukas/grounds/artifacts/native-pilot-<run-id>/`.

**Interfaces:** `run.py --variant <jvm|jvm-tuned|native-micro|native-distroless> --image <digest> --scenario <json> --output <dir>` liefert Messmanifest und JSONL-Rohwerte. `summarize.py --input <dir> --output <json>` liefert je Variante und Phase Requestzahl, angebotene/erreichte Rate, Fehler, CPU/Erfolg, Working-Set/RSS und p95/p99.

- [ ] Auswertung zuerst mit kleinen festen Datensätzen testen: Quantile, MiB-Umrechnung, CPU-Deltas, Null-Requests, fehlende Messpunkte und fehlgeschlagene Requests. Unvollständige oder unterlieferte Lastläufe dürfen nicht als bestanden erscheinen.
- [ ] Lasttreiber auf separater Ressource ausführen; vier Varianten seriell auf gleicher Anwendungsressource, Reihenfolge zwischen Wiederholungen wechseln. DB-Datensatz für jede Wiederholung identisch zurücksetzen. Sidecar und Telemetrie gleich konfigurieren.
- [ ] Drei Wiederholungen der exakten Spec-Phasen durchführen. Warmup getrennt ausweisen; Start bis Ready zusätzlich aus drei Kaltstarts messen. Metrics-Polling alle 15 Sekunden; Performanceergebnis nicht von vorhandenen JVM-Heap-Metriken abhängig machen.
- [ ] Lastprofil exakt übernehmen: 60/30/10 % bedingte App-Snapshots/Dokument-GETs/Namespace-Snapshots, 10 Namespaces mit je 10 Dokumenten, gleichverteilte Dokument-Reads und PUT-Writer mit disjunkten Schlüsseln. Den RAM-Vergleich als Median der drei zeitgewichteten Working-Set-Mittelwerte der regulären Lastphase berechnen.
- [ ] App und Sidecar getrennt auswerten. Distroless/Micro anhand identischen Binary-SHA256, RAM, komprimierter Transfergröße und bestandenem Funktionstest vergleichen; kleinere Bytes im Registry-Image nicht als RAM-Gewinn verbuchen.
- [ ] Abnahmegrenzen aus der Spec automatisiert prüfen und Ergebnis mit Rohdaten verlinken. Buildkosten und Diagnostizierbarkeit in die Entscheidung aufnehmen.

**Done:** Reproduzierbarer Vergleich aller vier Varianten mit Pass/Fail und dokumentierter Basiswahl. Bei nicht erreichtem Nutzen keine weiteren Services umstellen.

## Task 5: Regionaler Stage-Kandidat, Beobachtung und Rollback

**Files:** neu `deploy/experiments/service-config-native/{application.yaml,values.yaml,README.md}` im Deployment-Repo als ausdrücklich nicht automatisch aktivierter Kandidat. Falls erforderlich im Chart-Repo `charts/grounds-service/{values.yaml,templates/deployment.yaml,Chart.yaml}` sowie fokussierter Helm-Render-Test für Probes. Die gemeinsame `deploy/environments/stage/components/service-config/values.yaml` wird im Pilot nicht geändert.

**Interfaces:** Eigenständige ArgoCD-Application `service-config-native-pilot-de-nbg1` ohne Generator; Destination ist ausschließlich der vorab verifizierte registrierte Nürnberger Cluster. Eigenes Helm-Release/Service/Selektor `service-config-native-pilot`, bestehender ServiceAccount/Token-/Mesh-Kontext wird wiederverwendet. `serviceAccount.create=false` vermeidet den Eigentümerwechsel bestehender Ressourcen; der Chart muss den angegebenen bestehenden Account trotzdem setzen (falls nötig im selben Probe-Chart-Task gezielt ergänzen und rendern). Zwei dedizierte Velocity-Testinstanzen zeigen explizit auf die Kandidaten-URL. Rollback stellt diese Consumer auf die unveränderte JVM-URL zurück und deaktiviert die Kandidaten-Application.

- [ ] Aktuellen veröffentlichten Chart prüfen: Startup-/Readiness-Probes müssen tatsächlich gerendert werden. Falls nötig optionale Probe-Unterstützung ergänzen und testen: gesetzt → auf Port 9000 vorhanden; ungesetzt → bisheriger Chart-Output unverändert. Kandidat nutzt `/q/health/ready` sowie einen nachgewiesen verfügbaren Startup-Endpunkt.
- [ ] Live-Digest, Region, Deployment-Spezifikation und Quota-Raum sichern. Standalone-Application gegen den verifizierten Nürnberger ArgoCD-Zieleintrag rendern. Diff muss ausschließlich Kandidatenressourcen zeigen; kein Generator und keine Änderung am gemeinsamen stage-ApplicationSet oder Standard-Pin.
- [ ] Kandidat getrennt adressieren; eigene Test-App und zwei dedizierte Velocity-Testinstanzen mit explizitem Config-Endpunkt nutzen. Echte Stage-Abhängigkeiten nicht absichtlich unterbrechen. Beide Testproxies auf Snapshot-/Event-Konvergenz prüfen.
- [ ] Erst nach bestandenen Funktions-/Lasttests die Testconsumer auf den Kandidaten zeigen lassen. Bestehende JVM-Instanz und Erreichbarkeit des alten Digests erhalten. Spielertraffic bleibt auf JVM; initial keine pauschal kleineren Limits verteilen.
- [ ] Rollback tatsächlich proben: Testconsumer auf JVM-URL zurück, Config-Werte und Versionen unverändert, beide Proxy-Consumer funktionsfähig. Danach Kandidat erneut gezielt aktivieren und 24 Stunden beobachten.
- [ ] Bei OOM, neuen unerwarteten Fehlern, verletzten Latenzgrenzen oder fehlender Readiness zurückrollen. Nach erfolgreicher Beobachtung Request/Limit aus Peak inklusive Startup und Reserve ableiten; in einer separaten Änderung testen.
- [ ] Zweite Region nur mit zwei erreichbaren Clustern als geprüft markieren. Nach bestandenem Pilot einen getrennten Live-Rollout mit konkreten regionalen Overrides planen. Ergebnis dokumentieren und Folgepläne für Social, Player/Permissions sowie Match/Moderation priorisieren. Jeder Service behält einen eigenen Kompatibilitäts- und Netzwerk-Nachweis.

**Done:** Entweder regional validierter, rückrollbarer Native-Pilot mit belegter Einsparung oder begründete Entscheidung für die JVM. Ein erfolgreicher Build allein schließt den Pilot nicht ab.

## Umsetzung und Review

Ausführung im bestehenden Chat durch den primären Agenten, sobald der Entwurf durchgesehen ist. Keine parallele Arbeit an denselben Build-/Deployment-Dateien. Für diesen eng verbundenen Pilot ist ein durchgehender Implementierer sinnvoll; kritische Auth-/Netzwerk-/Rollout-Prüfung erfolgt gesondert auf dem stärkeren Modell. Dieser Plan löst keine Portal-Veröffentlichung oder sonstige Release-Aktion aus.
