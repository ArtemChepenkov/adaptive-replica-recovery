package ru.nsu.replicarecovery.runner.config;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import ru.nsu.replicarecovery.model.*;
import ru.nsu.replicarecovery.runner.client.*;
import ru.nsu.replicarecovery.runner.lab.*;

/** Explicit opt-in simulation. It performs no HTTP, Docker, Kafka or resource operations. */
@Configuration(proxyBeanMethods = false)
@Profile("test-lab")
public class TestLabConfiguration {
  @Bean Simulation simulation(Clock clock) { return new Simulation(clock); }

  @Bean LoadGeneratorClient simulatedLoad(Simulation simulation) { return simulation.load(); }
  @Bean CruiseControlClient simulatedCruise(Simulation simulation) { return simulation.cruise(); }
  @Bean PrometheusClient simulatedMonitoring() {
    return () -> new PrometheusClient.ResourceMetrics(10.0, 1024.0, 2048.0);
  }
  @Bean @Primary LabController simulatedLab() {
    return new LabController() {
      public LabValidation validate(ExperimentSpec spec) { return LabValidation.ok(); }
      public void prepare(ExperimentSpec spec) { }
      public void injectFault(FaultSpec fault) { }
      public CleanupResult cleanup() { return CleanupResult.ok(); }
    };
  }

  static final class Simulation {
    final Clock clock;
    volatile ExperimentSpec spec;
    volatile String loadId;
    volatile boolean running;
    volatile boolean recovering;
    int polls;
    Simulation(Clock clock) { this.clock = clock; }

    LoadGeneratorClient load() {
      return new LoadGeneratorClient() {
        public LoadStatus start(ExperimentSpec request) {
          spec = request; loadId = "simulated-load-" + UUID.randomUUID(); running = true; polls = 0;
          return status();
        }
        public LoadStatus status() { return new LoadStatus(running ? "RUNNING" : "STOPPED", loadId, !running, 0, null); }
        public LoadStatus stop() { running = false; return status(); }
        public LoadMetricsSnapshot metrics() {
          var now = clock.instant();
          return new LoadMetricsSnapshot(now.minusSeconds(60), now, now, 1000, 10.0, 50.0, 90.0,
              0, 0, 100, 100, 1024000, 0, new ControlSettingsSnapshot(spec.slo(), spec.recovery()));
        }
      };
    }

    CruiseControlClient cruise() {
      return new CruiseControlClient() {
        public String startRecovery(ExperimentSpec request) { recovering = true; polls = 0; return "simulated-task-" + UUID.randomUUID(); }
        public RecoveryControlStatus status() {
          if (recovering && ++polls >= 3) recovering = false;
          long throttle = spec == null ? 0 : Math.multiplyExact(spec.recovery().initialThrottleMiBps(), 1048576L);
          return new RecoveryControlStatus(!recovering, false, recovering, throttle,
              recovering ? 10485760L : 0L, 1048576L, recovering ? 1 : 0, !recovering,
              "SIMULATED", Map.of("simulated", true, "action", "HOLD", "reason", "SIMULATED",
                  "decisionTimestampMs", clock.instant().toEpochMilli()));
        }
        public void stopRecovery() { recovering = false; }
      };
    }
  }
}
