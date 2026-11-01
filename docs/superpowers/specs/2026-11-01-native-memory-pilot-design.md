# Config Native Image Pilot — Designentwurf

Status: Vorschlag zur Durchsicht. Dieser Auftrag erstellt den Plan; er startet keine Builds oder Deployments.

## Ziel und Ausgangslage

Den RAM-Bedarf der Grounds-Backend-Services senken und mit `service-config` nachweisen, ob Native Image den zusätzlichen Build- und Betriebsaufwand rechtfertigt. Der Pilot liefert wiederholbare Messungen und einen rückrollbaren Deployment-Kandidaten.

Die Messung vom 1. November 2026 im Cluster `game-de-nbg1` ergab für neun Service-Container 2,31 GiB Working Set plus 0,30 GiB Istio-Sidecars. Config lag bei 239 MiB, im verfügbaren 24h-Abfragefenster maximal bei 280 MiB. Ein Spieler war verbunden; daraus lässt sich kein Produktions-Lastmaximum ableiten. Rohdaten: `/home/lukas/grounds/artifacts/ram-evaluation-2026-11-01/measurements.json`.

GitHub bestätigt `service-config` v1.5.0 / main als Commit `7ac9de91ce025a33c3afd9efdaa3ee0450c6257f`. Der Deployment-Pin ist `1.5.0@sha256:27284c426ba33a7bea5b166018f2a3bc42beb4d1d9f41bff4138b74707be0862`. Bei Ausführung die Image-Provenienz gegen den Commit verifizieren. Der lokale Checkout ist älter und enthält eine ungetrackte `grounds.yaml`; diese darf nicht überschrieben werden.

Der verifizierte Stand nutzt Java 25, Quarkus 3.40.1, HTTP/JSON, PostgreSQL/Agroal/Flyway, NATS und JWT/JWKS. Die gRPC-Erweiterung erzeugt noch interne Nachrichtentypen, stellt aber keine öffentliche gRPC-API mehr bereit. Aktuell läuft ein Uber-JAR auf Distroless Java 25.

## Ansätze und vorgeschlagene Entscheidung

1. **JVM optimieren:** geringster Integrationsaufwand, behält JVM-Werkzeuge; Klassenmetadaten und JIT benötigen weiterhin RAM.
2. **Native-Pilot mit Vergleich gegen JVM:** zusätzlicher Build- und Kompatibilitätsaufwand, adressiert den gemessenen JVM-Grundbedarf. Das ist der vorgeschlagene Weg und die Grundlage dieses Plans.
3. **Sofort alle Services umstellen oder zusammenlegen:** vor dem Pilot fehlen belastbare Nutzen- und Kompatibilitätsdaten; regionale Aufgaben und Netzwerkverhalten machen eine Konsolidierung zu einem separaten Architekturprojekt.

Zuerst Config; weitere Services erhalten erst nach dem Ergebnis eigene Pläne. Im Pilot bleiben öffentliche API, Datenbankschema und Delivery-Semantik unverändert.

## Distroless und Build

Ein nativer Build produziert ein Linux-amd64-Binary mit gepinntem, zu Java 25 und Quarkus 3.40.1 kompatiblem Mandrel-/GraalVM-Builder. Builder-Version und ABI werden vor dem ersten Build geprüft und als Digests gespeichert; keine implizite Auswahl über `latest`.

Das identische Binary wird mit zwei Basen verpackt: Quarkus Micro als Kompatibilitätsreferenz und `gcr.io/distroless/cc-debian13:nonroot` als Kandidat. Beide Basen werden per Digest gepinnt. Die konkrete Kombination muss `glibc`, `libstdc++`, zlib, DNS und TLS vollständig unterstützen. Bibliotheken werden nicht nach Gutdünken aus fremden Distributionen kopiert. Wenn Distroless zusätzliche fragile Anpassungen braucht, bleibt Micro die Pilotbasis.

Distroless reduziert Image-Inhalt, Transfer und unnötige Programme. Der große erwartete RAM-Gewinn entsteht durch Native Image; ein zusätzlicher RAM-Gewinn durch Distroless wird erst nach Messung behauptet. Die vorhandene Distroless-Java-Basis wird für den nativen Prozess durch eine Basis ohne JVM ersetzt. Kein Wechsel auf musl/static/scratch und keine UPX-Kompression im Pilot.

Der JVM-Dockerfile und die vorhandenen Tags bleiben nutzbar. Native-Images erhalten eigene immutable Tags `native-sha-<commit>`; sie ersetzen keine vorhandenen JVM-Tags. Der Pilot baut in CI, nicht auf dem Game-Node.

## Funktion und Netzwerk

Die Tests laufen gegen echte Prozesse mit PostgreSQL, PgBouncer, NATS und einem HTTPS-JWKS-Testissuer. Mockito-Tests allein sind kein Native-Nachweis. Default-Datasource geht durch den Pooler, Migrationen weiterhin direkt an PostgreSQL. Keine neuen SQL-Migrationen.

Die aktuellen HTTP-Verträge einschließlich ETags, bedingter Writes und Authentifizierung gelten identisch für JVM und Native. NATS-Events bleiben best-effort: Nach Commit darf ein fehlender Hinweis die gespeicherten Daten nicht verlieren. Ein Consumer, der das Event verpasst, muss durch Snapshot-Polling konvergieren.

Mindestens zwei voneinander unabhängige Proxy-Consumer verwenden dieselbe Test-App mit unterschiedlichen Identitäten. Beide sehen Änderungen, auch wenn sie unterschiedliche Config-Instanzen ansprechen. Ein zusätzlicher Test benutzt zwei über Leaf/Hub verbundene NATS-Broker. Ein echter regionenübergreifender Stage-Test wird nur als bestanden ausgewiesen, wenn zwei erreichbare Game-Cluster verfügbar sind. Fehlt der zweite Cluster, bleibt diese Ausweitung gesperrt; der Ein-Region-Pilot kann trotzdem bewertet werden.

## Messung und Entscheidung

Vier Varianten desselben Kandidaten-Commits: unveränderte JVM-Konfiguration, optimierte JVM, Native/Micro, dasselbe Native-Binary/Distroless. Gleicher Datenbestand, CPU-Rahmen, Poolgröße, Sidecar und Telemetrie. Das historische Live-Image dient nur als zusätzliche Referenz.

Pro Variante: drei Durchläufe mit 5 Minuten Warmup, 10 Minuten Idle, 15 Minuten regulärer Last (100 Reads/s, 5 Writes/s) und 5 Minuten Burst (200 Reads/s, 10 Writes/s), jeweils mit 100 vorab angelegten Dokumenten à 4 KiB JSON. Reads verteilen sich auf 60 % App-Snapshot mit clientseitig gespeichertem `If-None-Match`, 30 % Dokument-GET und 10 % Namespace-Snapshot. Dokument-GETs wählen gleichverteilt unter den 100 Schlüsseln; zehn Namespaces enthalten je zehn Dokumente. Writes sind ausschließlich erfolgreiche PUTs: maximal 25 Writer verwalten disjunkte Dokumentschlüssel und aktualisieren ihre erwartete Dokumentversion nach jedem Erfolg. Absichtliche Versionskonflikte werden separat funktional getestet und nicht in diesem Lastprofil erzeugt. Dies ist ein synthetischer Vergleich, keine Zusage über benötigte Spielerkapazität. Lasttreiber auf einem separaten Rechner/Pod, Rate und Fehler explizit erfassen; bei Sättigung sind nicht erreichte Raten sichtbar und der Lauf besteht nicht.

Messen: Working Set und RSS je Container, CPU-Sekunden, p95/p99 nach Operation, Erfolgs-/Konflikt-/Fehlerraten, Start bis Ready, Image-Transfergröße, Builddauer und Build-RAM. Im Cluster werden cgroup-Metriken auch für Native verwendet; JVM-Heap-Metriken sind keine universellen Abnahmemetriken.

Vorgeschlagene Abnahmegrenzen:

- Alle funktionalen Tests bestehen auf der ausgewählten Runtime-Basis; keine OOMs oder unerwarteten Restarts. Ein gescheiterter Distroless-Vergleich wird mit Ursache verworfen und sperrt einen vollständig bestandenen Micro-Kandidaten nicht.
- Native spart mindestens 30 % App-Working-Set gegenüber der unveränderten JVM desselben Commits und mindestens 15 % gegenüber der optimierten JVM. Vergleichswert ist jeweils der Median der drei zeitgewichteten Working-Set-Mittelwerte über die reguläre Lastphase. Sidecars werden separat ausgewiesen.
- p95 steigt pro Operation höchstens um `max(10 % des JVM-Werts, 2 ms)`, p99 höchstens um `max(20 % des JVM-Werts, 5 ms)` gegenüber der unveränderten JVM. Keine zusätzlichen unerwarteten HTTP-/Transportfehler; absichtliche Konflikte/Auth-Fehler zählen separat.
- CPU-Sekunden pro erfolgreicher Operation steigen bei regulärer Last höchstens um 20 %. Der Burst erreicht die angebotene Rate ohne unbeschränkt wachsende Warteschlange.
- Nach 24 Stunden Stage-Betrieb keine zusätzliche Restart-/OOM- oder Fehlerhäufung; synthetische Daten werden getrennt von echten Config-Dokumenten gehalten.

Diese Grenzen sind Vorschläge für die Entscheidung, keine bereits erfüllten Eigenschaften. Falls Native die optimierte JVM kaum schlägt, bleibt die JVM eine gültige Entscheidung.

Schlägt ein neuer Vertragstest bereits gegen die unveränderte JVM fehl, wird er als vorhandener Fehler dokumentiert. Die Native-Migration erweitert dann nicht stillschweigend den Funktionsumfang. Ein Fehler in Auth, Datenkonsistenz oder Netzwerk-Konvergenz sperrt die Stage-Aktivierung bis zu einer separaten Korrektur oder ausdrücklich überarbeiteten Entscheidung; Vergleichsmessungen allein dürfen den Fehler nicht als Native-Erfolg überdecken.

## Deployment und Rollback

Zuerst isolierte Umgebung mit eigener Datenbank, Testdaten und NATS; keine Fehlersimulation auf gemeinsamen Stage-Abhängigkeiten. Danach ein getrennt adressierbarer Kandidat in Nürnberg. Aktivierung erfolgt durch genau eine eigenständige ArgoCD-Application `service-config-native-pilot-de-nbg1`, ohne Generator, mit explizit verifiziertem Nürnberger Destination-Cluster, dem bestehenden `grounds-service`-Chart und einem eigenen Helm-Release `service-config-native-pilot`. Ihre Experiment-Werte liegen außerhalb `environments/stage/components`. Vor Anwendung muss der gerenderte Umfang genau einen Kandidatenservice und dessen Deployment umfassen; keine ApplicationSet- oder Standard-Pin-Änderung.

Zwei dedizierte Velocity-Testinstanzen verwenden explizit die URL des Kandidaten und eine eigene Test-App. Der reguläre Config-Service und die echten Spielerproxies bleiben für den Pilot unverändert. Ein breiterer Live-Rollout braucht nach dem Pilot eine getrennte Änderung mit tatsächlich implementierten regionalen Overrides; das gemeinsame `values.yaml` wird im Pilot nicht umgepinnt.

Vor Routing auf den Kandidaten müssen HTTP-Readiness und Startup-Gates tatsächlich im gerenderten Deployment vorhanden sein. Der lokal geprüfte `grounds-service`-Chart rendert diese Probes bisher nicht; aktuelle veröffentlichte Chart-Version prüfen und erforderlichenfalls eng begrenzt ergänzen. Request/Limit erst nach belastbaren Messungen verkleinern; anfänglich bleibt der bisherige App-Rahmen erhalten.

Der bestehende JVM-Digest bleibt abrufbar. Pilot-Rollback stellt die Testconsumer auf die JVM-URL zurück und deaktiviert ausschließlich die eigenständige Kandidaten-Application. Rückkehr und Datenversionen werden vor der 24h-Beobachtung tatsächlich geprüft. Bei verändertem Readiness- oder Fehlerverhalten sofort zurückrollen; Tests mit NATS-Ausfall laufen nur isoliert. Unverändertes Schema erlaubt die Rückkehr ohne Datenmigration. Stage-Aktivierung ist eine spätere Ausführungsphase, keine Handlung dieses Planungsauftrags.

## Quellen

- [Quarkus Native Build und Trade-offs](https://quarkus.io/guides/building-native-image/)
- [Quarkus Micro Runtime und Bibliotheken](https://quarkus.io/guides/quarkus-runtime-base-image/)
- [Google Distroless: Varianten und Eigenschaften](https://github.com/GoogleContainerTools/distroless)
- [Native Reflection und Ressourcen](https://quarkus.io/guides/writing-native-applications-tips/)
