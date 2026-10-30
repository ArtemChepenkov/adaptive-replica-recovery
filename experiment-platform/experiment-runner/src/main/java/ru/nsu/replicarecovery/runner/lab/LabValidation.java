package ru.nsu.replicarecovery.runner.lab;

import java.util.List;

public record LabValidation(boolean valid, List<String> errors) {
  public LabValidation { errors = List.copyOf(errors); }
  public static LabValidation ok() { return new LabValidation(true, List.of()); }
}
