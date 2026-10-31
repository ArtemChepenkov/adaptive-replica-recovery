package ru.nsu.replicarecovery.runner.config;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import ru.nsu.replicarecovery.runner.client.*;
import ru.nsu.replicarecovery.runner.lab.LabController;
import ru.nsu.replicarecovery.runner.lifecycle.ExperimentCoordinator;
import ru.nsu.replicarecovery.runner.results.ExperimentResultStore;
import ru.nsu.replicarecovery.runner.metrics.MonitoringCollector;

@Configuration(proxyBeanMethods = false)
public class RunnerConfiguration {
  @Bean ExperimentSpecLoader experimentSpecLoader() { return new ExperimentSpecLoader(); }
  @Bean Clock runnerClock() { return Clock.systemUTC(); }
  @Bean ScenarioCatalog scenarioCatalog(@Value("${runner.scenarios-directory:../infrastructure/scenarios}") String path) {
    return new ScenarioCatalog(Path.of(path));
  }
  @Bean
  ExperimentCoordinator experimentCoordinator(ExperimentResultStore store, LoadGeneratorClient load,
      CruiseControlClient cruise, PrometheusClient monitor, LabController lab, Clock clock,
      @Value("${runner.poll-interval:1s}") String interval,
      @Value("${runner.cleanup-timeout:30s}") String cleanup, Environment environment) {
    return new ExperimentCoordinator(store, load, cruise, monitor, lab, clock,
        DurationStyle.detectAndParse(interval), DurationStyle.detectAndParse(cleanup), System::nanoTime,
        duration -> TimeUnit.NANOSECONDS.sleep(duration.toNanos()),
        environment.matchesProfiles("test-lab"));
  }

  static PrometheusClient monitoringClient(Environment environment, Clock clock) {
    var brokers = Arrays.stream(environment.getProperty("runner.monitoring.broker-ids", "1,2,3,4").split(","))
        .map(String::trim).map(Integer::valueOf).collect(Collectors.toSet());
    return new MonitoringCollector(environment.getProperty("runner.prometheus-url", "http://localhost:9091"),
        environment.getProperty("runner.cruise-url", "http://localhost:9090"),
        environment.getProperty("runner.monitoring.compose-project", "replica-recovery"), brokers, clock);
  }

  @Configuration(proxyBeanMethods = false)
  @Profile("!test-lab")
  static class HttpAdapters {
    @Bean @ConditionalOnMissingBean(LoadGeneratorClient.class)
    LoadGeneratorClient loadGeneratorClient(@Value("${runner.load-url:http://localhost:8081}") String url) {
      return new HttpLoadGeneratorClient(url);
    }
    @Bean @ConditionalOnMissingBean(CruiseControlClient.class)
    CruiseControlClient cruiseControlClient(@Value("${runner.cruise-url:http://localhost:9090}") String url) {
      return new HttpCruiseControlClient(url);
    }
    @Bean @ConditionalOnMissingBean(PrometheusClient.class)
    PrometheusClient prometheusClient(Environment environment, Clock clock) {
      return monitoringClient(environment, clock);
    }
  }
}
