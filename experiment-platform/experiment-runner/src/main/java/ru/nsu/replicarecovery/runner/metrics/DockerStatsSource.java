package ru.nsu.replicarecovery.runner.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Runs only docker ps and docker stats; no shell or mutating Docker operation. */
final class DockerStatsSource implements Supplier<DockerStatsSource.Reading> {
  private static final Pattern BYTES = Pattern.compile("([0-9]+(?:\\.[0-9]+)?)\\s*(B|kB|KB|MB|GB|TB|KiB|MiB|GiB|TiB)");
  private final String project;
  private final Set<Integer> expected;
  private final Clock clock;
  private final Command command;
  private final ObjectMapper json = new ObjectMapper();
  private final Map<Integer, Previous> previous = new HashMap<>();

  DockerStatsSource(String project, Set<Integer> expected, Clock clock) {
    this(project, expected, clock, DockerStatsSource::execute);
  }
  DockerStatsSource(String project, Set<Integer> expected, Clock clock, Command command) {
    if (project == null || !project.matches("[a-z0-9][a-z0-9_-]{0,62}")) {
      throw new IllegalArgumentException("invalid Compose project name");
    }
    this.project = project; this.expected = Set.copyOf(expected); this.clock = clock; this.command = command;
  }

  public synchronized Reading get() {
    Map<Integer, Double> cpu = new HashMap<>(), memory = new HashMap<>(), disk = new HashMap<>();
    List<String> errors = new ArrayList<>();
    try {
      String containers = command.run(List.of("docker", "ps", "--filter", "label=com.docker.compose.project=" + project,
          "--format", "{{.ID}} {{.Label \"com.docker.compose.service\"}}"));
      Map<String, Integer> ids = new HashMap<>();
      Set<Integer> brokers = new HashSet<>();
      for (String line : containers.lines().filter(value -> !value.isBlank()).toList()) {
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 2 || !fields[0].matches("[a-f0-9]{12,64}") || !fields[1].matches("broker-[1-9][0-9]*")) continue;
        int broker = Integer.parseInt(fields[1].substring(7));
        if (!expected.contains(broker)) continue;
        if (!brokers.add(broker)) throw new IllegalStateException("duplicate broker containers");
        ids.put(fields[0], broker);
      }
      if (!ids.isEmpty()) {
        List<String> statsCommand = new ArrayList<>(List.of("docker", "stats", "--no-stream", "--format", "{{json .}}"));
        statsCommand.addAll(ids.keySet());
        Set<Integer> seen = new HashSet<>();
        for (String line : command.run(List.copyOf(statsCommand)).lines().filter(value -> !value.isBlank()).toList()) {
          try {
            JsonNode stat = json.readTree(line);
            String container = stat.path("ID").asText();
            Integer broker = ids.get(container);
            if (broker == null || !seen.add(broker)) throw new IllegalArgumentException("unexpected stats identity");
            double cpuValue = Double.parseDouble(stat.path("CPUPerc").asText().replace("%", "").trim());
            double memoryValue = bytes(stat.path("MemUsage").asText().split("/", -1)[0].trim());
            String[] io = stat.path("BlockIO").asText().split("/", -1);
            if (!Double.isFinite(cpuValue) || cpuValue < 0 || io.length != 2) throw new IllegalArgumentException("invalid stats");
            double written = bytes(io[1].trim());
            cpu.put(broker, cpuValue); memory.put(broker, memoryValue);
            Instant now = clock.instant();
            Previous last = previous.put(broker, new Previous(container, written, now));
            double elapsed = last == null ? 0 : Duration.between(last.time(), now).toNanos() / 1_000_000_000.0;
            if (last != null && last.container().equals(container) && elapsed > 0 && elapsed <= 15 && written >= last.written()) {
              disk.put(broker, (written - last.written()) / elapsed);
            } else {
              errors.add("DOCKER_DISK_BASELINE_UNAVAILABLE:" + broker);
            }
          } catch (RuntimeException | IOException exception) { errors.add("DOCKER_STATS_INVALID"); }
        }
      }
      for (int broker : expected) {
        if (!cpu.containsKey(broker)) { previous.remove(broker); errors.add("DOCKER_BROKER_MISSING:" + broker); }
      }
    } catch (Exception exception) {
      if (exception instanceof InterruptedException) Thread.currentThread().interrupt();
      previous.clear(); errors.add("DOCKER_UNAVAILABLE");
    }
    return new Reading(Map.copyOf(cpu), Map.copyOf(memory), Map.copyOf(disk), List.copyOf(errors));
  }

  private static double bytes(String text) {
    Matcher match = BYTES.matcher(text);
    if (!match.matches()) throw new IllegalArgumentException("invalid Docker byte value");
    String unit = match.group(2);
    int power = switch (unit.charAt(0)) { case 'k', 'K' -> 1; case 'M' -> 2; case 'G' -> 3; case 'T' -> 4; default -> 0; };
    double value = Double.parseDouble(match.group(1)) * Math.pow(unit.contains("i") ? 1024 : 1000, power);
    if (!Double.isFinite(value) || value < 0) throw new IllegalArgumentException("invalid Docker bytes");
    return value;
  }

  private static String execute(List<String> arguments) throws Exception {
    Process process = new ProcessBuilder(arguments).redirectErrorStream(true).start();
    try {
      if (!process.waitFor(2900, TimeUnit.MILLISECONDS)) throw new IOException("Docker stats deadline exceeded");
      byte[] output = process.getInputStream().readNBytes(65537);
      if (process.exitValue() != 0 || output.length > 65536) throw new IOException("Docker stats unavailable");
      return new String(output, StandardCharsets.UTF_8);
    } finally {
      if (process.isAlive()) process.destroyForcibly();
    }
  }
  @FunctionalInterface interface Command { String run(List<String> arguments) throws Exception; }
  record Reading(Map<Integer, Double> cpuPercent, Map<Integer, Double> memoryBytes,
      Map<Integer, Double> diskWriteBytesPerSecond, List<String> errors) { }
  private record Previous(String container, double written, Instant time) { }
}
