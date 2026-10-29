package ru.nsu.replicarecovery.runner;

import java.nio.file.Path;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import ru.nsu.replicarecovery.runner.lab.LabController;
import ru.nsu.replicarecovery.runner.lifecycle.RestartRecovery;
import ru.nsu.replicarecovery.runner.results.ExperimentResultStore;
import ru.nsu.replicarecovery.runner.results.FileExperimentResultStore;

@SpringBootApplication
public class ExperimentRunnerApplication {
  public static void main(String[] args) {
    SpringApplication.run(ExperimentRunnerApplication.class, args);
  }

  @Bean
  ExperimentResultStore experimentResultStore(@Value("${runner.results-directory:../results}") String root) {
    return new FileExperimentResultStore(Path.of(root), Clock.systemUTC());
  }

  @Bean
  @ConditionalOnMissingBean(LabController.class)
  LabController unavailableLabController() {
    return () -> { throw new IllegalStateException("Lab adapter is not configured; implemented in Task 6"); };
  }

  @Bean
  SmartInitializingSingleton recoverInterruptedExperiments(ExperimentResultStore store, LabController lab) {
    return () -> new RestartRecovery(store, lab).recover();
  }
}
