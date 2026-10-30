package ru.nsu.replicarecovery.runner.config;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
    PrometheusClient prometheusClient(@Value("${runner.prometheus-url:http://localhost:9091}") String url,
        @Value("${runner.prometheus-cpu-query:avg(broker_cpu_percent)}") String cpu,
        @Value("${runner.prometheus-network-query:sum(rate(broker_network_out_bytes_total[1m]))}") String network,
        @Value("${runner.prometheus-disk-query:sum(rate(broker_disk_write_bytes_total[1m]))}") String disk) {
      return new HttpPrometheusClient(url, cpu, network, disk);
    }
  }
}
