package ru.nsu.replicarecovery.runner.lab;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentSpecValidator;
import ru.nsu.replicarecovery.model.FaultSpec;

/** Real, single-experiment Docker lab adapter. Operation scripts scope themselves to the lab project. */
public final class DockerLabController implements LabController {
  private static final BiConsumer<String, String> NO_EVENTS = (severity, message) -> { };
  private final ResourceProfileApplier profiles;
  private final FaultActionExecutor faults;
  private final LabCleanupService cleanup;
  private final LongSupplier nanos;
  private BiConsumer<String, String> events = NO_EVENTS;
  private ExperimentSpec validated;
  private ResourceProfileScheduler scheduler;
  private Instant observedFaultAt;

  public DockerLabController(Path projectRoot, Duration timeout) {
    this(new CommandRunner(projectRoot, timeout));
  }
  public DockerLabController(CommandRunner commands) { this(commands, System::nanoTime); }
  DockerLabController(CommandRunner commands, LongSupplier nanos) {
    this.nanos = nanos;
    profiles = new ResourceProfileApplier(commands, this::event);
    faults = new FaultActionExecutor(commands, this::event);
    cleanup = new LabCleanupService(commands, this::event);
  }

  @Override public void beginExperiment(String id, BiConsumer<String, String> events) {
    if (scheduler != null) scheduler.cancel();
    scheduler = null; validated = null; observedFaultAt = null;
    this.events = events == null ? NO_EVENTS : events;
  }
  @Override public LabValidation validate(ExperimentSpec spec) {
    validated = null;
    var errors = new ArrayList<String>();
    try {
      new ExperimentSpecValidator().validate(spec);
      var names = new LinkedHashSet<String>();
      names.add(spec.resources().profile());
      for (var change : spec.resources().schedule()) names.add(change.profile());
      for (String name : names) {
        var result = profiles.validate(name);
        if (!result.valid()) result.errors().forEach(error -> errors.add(name + ": " + error));
      }
      errors.addAll(faults.validate(spec.fault()).errors());
      // Validate conversion/range/order before warm-up as well as before scheduler construction.
      new ResourceProfileScheduler(spec.resources().schedule(), profiles::apply, nanos);
      if (errors.isEmpty()) validated = spec;
    } catch (RuntimeException exception) { errors.add("LAB_VALIDATION_FAILED: " + exception.getMessage()); }
    return new LabValidation(errors.isEmpty(), errors);
  }
  @Override public void prepare(ExperimentSpec spec) {
    if (!spec.equals(validated)) throw new IllegalStateException("Lab must be validated before preparation");
    profiles.apply(spec.resources().profile());
    scheduler = new ResourceProfileScheduler(spec.resources().schedule(), profiles::apply, nanos);
  }
  @Override public void loadStarted() {
    if (scheduler == null) throw new IllegalStateException("Lab must be prepared before load starts");
    scheduler.start();
    event("INFO", "RESOURCE_TIMELINE_STARTED");
  }
  @Override public void tick() { if (scheduler != null) scheduler.tick(); }
  @Override public void injectFault(FaultSpec fault) {
    observedFaultAt = null;
    observedFaultAt = faults.execute(fault);
  }
  @Override public Instant observedFaultAt() { return observedFaultAt; }
  @Override public CleanupResult cleanup() {
    if (scheduler != null) scheduler.cancel();
    try { return cleanup.cleanup(); }
    finally { scheduler = null; validated = null; observedFaultAt = null; events = NO_EVENTS; }
  }
  private void event(String severity, String message) { events.accept(severity, message); }
}
