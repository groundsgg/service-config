package gg.grounds.events

import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.inject.Inject

@ApplicationScoped
class NatsLifecycle @Inject constructor(private val changePublisher: ConfigChangePublisher) {
    fun onStart(@Observes event: StartupEvent) {
        changePublisher.connect()
    }

    fun onStop(@Observes event: ShutdownEvent) {
        changePublisher.close()
    }
}
