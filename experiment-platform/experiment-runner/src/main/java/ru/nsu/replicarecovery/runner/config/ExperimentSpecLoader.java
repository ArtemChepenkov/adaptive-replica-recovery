package ru.nsu.replicarecovery.runner.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Pattern;
import org.springframework.boot.convert.DurationStyle;
import ru.nsu.replicarecovery.model.ExperimentSpec;
import ru.nsu.replicarecovery.model.ExperimentSpecValidator;

/**
 * Loads schema 1 without silently coercing strings, floating point integers or nulls.
 * Defaults are applied only to absent fields. Root seed defaults to 20260910; SLO budgets
 * to 2 minutes and 1000 ms-seconds; resource schedule to empty. Recovery requires policy
 * and initial/min/max throttle; the remaining policy defaults match the initial experiment
 * settings (5 MiB/s increase, 20 MiB/s maximum change, 0.7 decrease, 10 s control,
 * 60 s metrics, 30 s stale grace, initial-rate fallback, 0.85 healthy ratio, 2 healthy
 * windows, 100 observations, 20 s cooldown, 5 MiB/s progress floor, 1 MiB/s minimum
 * progress, 3 no-progress windows and 3 copies/domains). Adaptation defaults to true
 * for AIMD only; risk ordering defaults to true. These defaults are not calibration.
 */
public final class ExperimentSpecLoader {
  private static final Pattern UNIT_DURATION = Pattern.compile("[+-]?\\d+(ns|us|ms|s|m|h|d)");
  private final ObjectMapper mapper;
  private final ExperimentSpecValidator validator = new ExperimentSpecValidator();

  public ExperimentSpecLoader() {
    YAMLFactory factory = new YAMLFactory();
    factory.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    SimpleModule durationModule = new SimpleModule();
    durationModule.addDeserializer(Duration.class, new JsonDeserializer<>() {
      @Override
      public Duration deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_STRING) {
          return context.reportInputMismatch(Duration.class,
              "duration must be a unit-bearing string (30m, 10s) or ISO-8601 (PT30M)");
        }
        String value = parser.getText().trim();
        if (!UNIT_DURATION.matcher(value).matches()
            && !value.startsWith("P") && !value.startsWith("-P") && !value.startsWith("+P")) {
          throw context.weirdStringException(value, Duration.class,
              "duration must include a unit or use ISO-8601");
        }
        try {
          return DurationStyle.detectAndParse(value);
        } catch (RuntimeException exception) {
          throw context.weirdStringException(value, Duration.class, "invalid duration");
        }
      }
    });
    mapper = YAMLMapper.builder(factory)
        .addModule(new JavaTimeModule())
        .addModule(durationModule)
        .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
        .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
        .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
        .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .build();
  }

  public ExperimentSpec load(Path path) {
    try {
      return load(Files.readString(path, StandardCharsets.UTF_8));
    } catch (IOException exception) {
      throw new UncheckedIOException("cannot read experiment configuration: " + path, exception);
    }
  }

  public ExperimentSpec load(String yaml) {
    if (yaml == null || yaml.isBlank()) {
      throw new IllegalArgumentException("experiment document must not be empty");
    }
    try {
      JsonNode tree = mapper.readTree(yaml);
      if (!(tree instanceof ObjectNode root)) {
        throw new IllegalArgumentException("experiment document must be an object with schemaVersion");
      }
      JsonNode schema = root.get("schemaVersion");
      if (schema == null || !schema.isIntegralNumber() || !schema.canConvertToInt()
          || schema.intValue() != 1) {
        throw new IllegalArgumentException("schemaVersion must be integer 1");
      }
      applyDefaults(root);
      ExperimentSpec spec = mapper.treeToValue(root, ExperimentSpec.class);
      validator.validate(spec);
      return spec;
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("invalid experiment configuration: " + exception.getMessage(), exception);
    }
  }

  private void applyDefaults(ObjectNode root) {
    if (!root.has("seed")) {
      root.put("seed", 20260910L);
    }
    ObjectNode slo = requiredObject(root, "slo");
    if (!slo.has("maxViolationTime")) {
      slo.put("maxViolationTime", "PT2M");
    }
    if (!slo.has("maxViolationAreaMsSeconds")) {
      slo.put("maxViolationAreaMsSeconds", 1000);
    }
    ObjectNode resources = requiredObject(root, "resources");
    if (!resources.has("schedule")) {
      resources.putArray("schedule");
    }
    ObjectNode recovery = requiredObject(root, "recovery");
    for (String field : new String[] {
        "throttlePolicy", "initialThrottleMiBps", "minThrottleMiBps", "maxThrottleMiBps"}) {
      if (!recovery.hasNonNull(field)) {
        throw new IllegalArgumentException("recovery." + field + " is required and must not be null");
      }
    }
    ObjectNode defaults = mapper.createObjectNode()
        .put("adaptiveThrottleEnabled", "AIMD".equalsIgnoreCase(recovery.get("throttlePolicy").asText()))
        .put("riskOrderingEnabled", true)
        .put("additiveStepMiBps", 5)
        .put("maxChangeMiBps", 20)
        .put("multiplicativeDecrease", 0.7)
        .put("controlInterval", "PT10S")
        .put("metricsWindow", "PT60S")
        .put("staleGrace", "PT30S")
        .put("healthySloRatio", 0.85)
        .put("healthyWindows", 2)
        .put("minimumObservations", 100)
        .put("cooldown", "PT20S")
        .put("progressFloorMiBps", 5)
        .put("minProgressMiBps", 1)
        .put("noProgressWindows", 3)
        .put("requiredCopies", 3)
        .put("requiredDomains", 3);
    defaults.set("safeFallbackThrottleMiBps", recovery.get("initialThrottleMiBps"));
    defaults.fields().forEachRemaining(entry -> {
      if (!recovery.has(entry.getKey())) {
        recovery.set(entry.getKey(), entry.getValue());
      }
    });
  }

  private static ObjectNode requiredObject(ObjectNode root, String field) {
    JsonNode value = root.get(field);
    if (!(value instanceof ObjectNode object)) {
      throw new IllegalArgumentException(field + " must be an object");
    }
    return object;
  }
}
