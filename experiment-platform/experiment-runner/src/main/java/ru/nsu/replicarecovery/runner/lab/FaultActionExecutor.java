package ru.nsu.replicarecovery.runner.lab;

import java.util.List;
import java.util.function.BiConsumer;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import ru.nsu.replicarecovery.model.FaultSpec;

/** A script succeeds only after observing the requested fault, not just sending a command. */
public final class FaultActionExecutor {
  private final CommandRunner commands;
  private final BiConsumer<String, String> events;
  public FaultActionExecutor(CommandRunner commands, BiConsumer<String, String> events) {
    this.commands = commands; this.events = events;
  }
  public LabValidation validate(FaultSpec fault) {
    try {
      var result = commands.run(CommandRunner.Operation.FAULT_VALIDATE, fault.action().name(), fault.target());
      return result.success() ? LabValidation.ok() : new LabValidation(false, result.errors());
    } catch (RuntimeException exception) {
      return new LabValidation(false, List.of("FAULT_VALIDATION_FAILED: " + exception.getMessage()));
    }
  }
  public Instant execute(FaultSpec fault) {
    LabValidation validation = validate(fault);
    if (!validation.valid()) throw new IllegalStateException("FAULT_UNSUPPORTED: " + validation.errors());
    events.accept("INFO", "FAULT_REQUESTED " + fault.action() + " " + fault.target());
    var result = commands.run(CommandRunner.Operation.FAULT_APPLY, fault.action().name(), fault.target());
    result.requireSuccess("FAULT_APPLY " + fault.action());
    var field = result.body().path("details").path("faultObservedAt");
    if (!field.isTextual()) throw new IllegalStateException("Fault result must contain faultObservedAt");
    Instant observed;
    try { observed = Instant.parse(field.textValue()); }
    catch (DateTimeParseException exception) { throw new IllegalStateException("Fault observation timestamp is invalid", exception); }
    events.accept("INFO", "FAULT_CONFIRMED " + fault.action() + " " + fault.target()
        + " observedAt=" + observed + " " + result.stdout());
    return observed;
  }
}
