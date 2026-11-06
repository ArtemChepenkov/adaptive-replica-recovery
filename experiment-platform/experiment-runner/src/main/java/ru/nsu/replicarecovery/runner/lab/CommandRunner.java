package ru.nsu.replicarecovery.runner.lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import ru.nsu.replicarecovery.model.FaultAction;

/** The application can execute only fixed lab scripts and validated operation arguments. */
public final class CommandRunner {
  private static final int OUTPUT_LIMIT = 131072;
  private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final Path root;
  private final Duration timeout;
  private final Executor executor;

  public enum Operation {
    PROFILE_VALIDATE("apply-profile.sh", "validate", 1),
    PROFILE_APPLY("apply-profile.sh", "apply", 1),
    PROFILE_STATUS("apply-profile.sh", "status", 0),
    FAULT_VALIDATE("inject-fault.sh", "validate", 2),
    FAULT_APPLY("inject-fault.sh", "apply", 2),
    RESET_RESOURCES("cleanup-lab.sh", "reset-resources", 0),
    START_BROKERS("cleanup-lab.sh", "start-brokers", 0),
    HEALTH("cleanup-lab.sh", "health", 0),
    REASSIGNMENTS("cleanup-lab.sh", "reassignments", 0);

    final String script;
    final String action;
    final int arguments;
    Operation(String script, String action, int arguments) {
      this.script = script; this.action = action; this.arguments = arguments;
    }
  }

  public CommandRunner(Path projectRoot, Duration timeout) {
    this(projectRoot, timeout, CommandRunner::execute);
  }

  CommandRunner(Path projectRoot, Duration timeout, Executor executor) {
    if (projectRoot == null || timeout == null || timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("project root and positive command timeout required");
    }
    this.root = projectRoot.toAbsolutePath().normalize();
    this.timeout = timeout;
    this.executor = executor;
  }

  public Result run(Operation operation, String... arguments) {
    if (operation == null || arguments == null || arguments.length != operation.arguments) {
      throw denied();
    }
    if (operation == Operation.FAULT_VALIDATE || operation == Operation.FAULT_APPLY) {
      try { FaultAction.valueOf(arguments[0]); }
      catch (RuntimeException exception) { throw denied(); }
      if (arguments[1] == null || !arguments[1].matches("(?:broker-[1-9][0-9]*|zone-[a-z0-9-]+)")) throw denied();
    } else {
      for (String argument : arguments) {
        if (argument == null || !argument.matches("[a-z0-9][a-z0-9-]{0,63}")) throw denied();
      }
    }
    Path script = root.resolve("infrastructure/scripts").resolve(operation.script).normalize();
    if (!script.startsWith(root) || !Files.isRegularFile(script) || Files.isSymbolicLink(script)) {
      throw new IllegalStateException("Whitelisted lab script is missing or is a symbolic link: " + operation.script);
    }
    try {
      Path realScript = script.toRealPath();
      if (!realScript.startsWith(root.toRealPath())) {
        throw new IllegalStateException("Whitelisted lab script escapes project root");
      }
      // Resolve parent directory symlinks before execution, not just the final file.
      List<String> command = new ArrayList<>(List.of("bash", realScript.toString(), operation.action));
      command.addAll(Arrays.asList(arguments));
      return executor.execute(List.copyOf(command), root, timeout);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Lab operation interrupted: " + operation, exception);
    } catch (IOException exception) {
      throw new IllegalStateException("Lab operation failed: " + operation + ": " + exception.getMessage(), exception);
    }
  }

  /** Compatibility entry point deliberately does not accept arbitrary executables. */
  public Result run(String script, String... arguments) {
    if (script == null || arguments == null || arguments.length == 0) throw denied();
    for (Operation operation : Operation.values()) {
      if (script.equals("infrastructure/scripts/" + operation.script) && arguments[0].equals(operation.action)) {
        return run(operation, Arrays.copyOfRange(arguments, 1, arguments.length));
      }
    }
    throw denied();
  }

  private static IllegalArgumentException denied() {
    return new IllegalArgumentException("Operation is not allowed");
  }

  private static Result execute(List<String> arguments, Path root, Duration timeout)
      throws IOException, InterruptedException {
    if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux")) {
      throw new IOException("Docker lab operations require Linux/WSL; run Runner inside WSL");
    }
    Process process = new ProcessBuilder(arguments).directory(root.toFile()).start();
    var stdout = capture(process.getInputStream());
    var stderr = capture(process.getErrorStream());
    try {
      if (!process.waitFor(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
        throw new IOException("Lab command deadline exceeded");
      }
      String out = stdout.get(2, TimeUnit.SECONDS);
      String err = stderr.get(2, TimeUnit.SECONDS);
      return new Result(process.exitValue(), out, err);
    } catch (InterruptedException exception) {
      throw exception;
    } catch (Exception exception) {
      if (exception instanceof IOException io) throw io;
      throw new IOException("Cannot read bounded lab command output", exception);
    } finally {
      // Bash/Python/Docker CLI descendants must not survive a timeout or cancellation.
      var descendants = process.descendants().toList();
      descendants.forEach(ProcessHandle::destroy);
      if (process.isAlive()) process.destroy();
      descendants.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
      if (process.isAlive()) process.destroyForcibly();
      try { process.getInputStream().close(); process.getErrorStream().close(); }
      catch (IOException ignored) { }
    }
  }

  private static CompletableFuture<String> capture(InputStream input) {
    CompletableFuture<String> result = new CompletableFuture<>();
    Thread reader = new Thread(() -> {
      try {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int count;
        boolean overflow = false;
        while ((count = input.read(buffer)) != -1) {
          if (output.size() + count <= OUTPUT_LIMIT) output.write(buffer, 0, count);
          else overflow = true;
        }
        if (overflow) throw new IOException("Lab command output limit exceeded");
        result.complete(output.toString(StandardCharsets.UTF_8));
      } catch (IOException exception) { result.completeExceptionally(exception); }
    }, "lab-command-output");
    reader.setDaemon(true);
    reader.start();
    return result;
  }

  @FunctionalInterface interface Executor {
    Result execute(List<String> command, Path root, Duration timeout) throws IOException, InterruptedException;
  }

  public record Result(int exitCode, String stdout, String stderr) {
    public JsonNode body() {
      try {
        JsonNode value = JSON.readTree(stdout);
        if (value == null || !value.isObject() || !value.path("success").isBoolean()
            || !value.path("errors").isArray()) throw new IOException("invalid lab script result");
        for (JsonNode error : value.path("errors")) {
          if (!error.isTextual()) throw new IOException("invalid script error");
        }
        return value;
      } catch (IOException exception) { throw new IllegalStateException("Lab script returned invalid JSON", exception); }
    }
    public boolean success() {
      JsonNode value = body();
      return exitCode == 0 && value.path("success").booleanValue()
          && !value.path("unsupported").asBoolean(false) && value.path("errors").isEmpty();
    }
    public List<String> errors() {
      JsonNode value = body();
      List<String> errors = new ArrayList<>();
      value.path("errors").forEach(error -> errors.add(error.textValue()));
      if (!success() && errors.isEmpty()) errors.add("Lab script unsuccessful (exit " + exitCode + ")");
      return List.copyOf(errors);
    }
    public void requireSuccess(String operation) {
      if (!success()) throw new IllegalStateException(operation + ": " + errors());
    }
  }
}
