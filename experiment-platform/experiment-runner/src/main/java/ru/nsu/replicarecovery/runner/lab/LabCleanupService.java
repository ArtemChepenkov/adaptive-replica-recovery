package ru.nsu.replicarecovery.runner.lab;

import java.util.ArrayList;
import java.util.function.BiConsumer;

/** Load and recovery are stopped by the coordinator before these physical-lab phases. */
public final class LabCleanupService {
  private final CommandRunner commands;
  private final BiConsumer<String, String> events;
  public LabCleanupService(CommandRunner commands, BiConsumer<String, String> events) {
    this.commands = commands; this.events = events;
  }
  public CleanupResult cleanup() {
    var errors = new ArrayList<String>();
    for (var operation : new CommandRunner.Operation[] {CommandRunner.Operation.RESET_RESOURCES,
        CommandRunner.Operation.START_BROKERS, CommandRunner.Operation.HEALTH, CommandRunner.Operation.REASSIGNMENTS}) {
      try {
        var result = commands.run(operation);
        result.requireSuccess(operation.name());
        events.accept("INFO", "LAB_CLEANUP_CONFIRMED " + operation + " " + result.stdout());
      } catch (RuntimeException exception) {
        errors.add(operation + ": " + exception.getMessage());
        try { events.accept("ERROR", "LAB_CLEANUP_FAILED " + operation + ": " + exception.getMessage()); }
        catch (RuntimeException ignored) { /* All remaining physical cleanup phases must still run. */ }
      }
    }
    return new CleanupResult(true, errors.isEmpty(), errors);
  }
}
