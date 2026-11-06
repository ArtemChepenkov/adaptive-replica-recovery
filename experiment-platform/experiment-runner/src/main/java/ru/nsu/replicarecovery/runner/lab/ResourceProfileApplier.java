package ru.nsu.replicarecovery.runner.lab;

import java.util.List;
import java.util.function.BiConsumer;

/** Script validation checks profile schema, target capabilities and matching admission proof. */
public final class ResourceProfileApplier {
  private final CommandRunner commands;
  private final BiConsumer<String, String> events;
  public ResourceProfileApplier(CommandRunner commands, BiConsumer<String, String> events) {
    this.commands = commands; this.events = events;
  }
  public LabValidation validate(String profile) {
    try {
      var result = commands.run(CommandRunner.Operation.PROFILE_VALIDATE, profile);
      return result.success() ? LabValidation.ok() : new LabValidation(false, result.errors());
    } catch (RuntimeException exception) {
      return new LabValidation(false, List.of("PROFILE_VALIDATION_FAILED " + profile + ": " + exception.getMessage()));
    }
  }
  public void apply(String profile) {
    LabValidation validation = validate(profile);
    if (!validation.valid()) throw new IllegalStateException("PROFILE_UNSUPPORTED " + profile + ": " + validation.errors());
    events.accept("INFO", "RESOURCE_PROFILE_REQUESTED " + profile);
    var result = commands.run(CommandRunner.Operation.PROFILE_APPLY, profile);
    result.requireSuccess("PROFILE_APPLY " + profile);
    events.accept("INFO", "RESOURCE_PROFILE_APPLIED " + profile + " " + result.stdout());
  }
}
