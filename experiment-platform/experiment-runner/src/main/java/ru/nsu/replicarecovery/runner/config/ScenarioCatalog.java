package ru.nsu.replicarecovery.runner.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Reads scenario texts only from the configured directory; request paths are never accepted. */
public final class ScenarioCatalog {
  private final Path root;

  public ScenarioCatalog(Path root) {
    this.root = root.toAbsolutePath().normalize();
  }

  public List<Profile> profiles() {
    if (!Files.exists(root)) {
      return List.of();
    }
    try (var files = Files.list(root)) {
      List<Profile> profiles = new ArrayList<>();
      for (Path path : files.sorted().toList()) {
        String filename = path.getFileName().toString();
        String lower = filename.toLowerCase(Locale.ROOT);
        if ((lower.endsWith(".yaml") || lower.endsWith(".yml"))
            && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
          profiles.add(new Profile(filename, Files.readString(path, StandardCharsets.UTF_8)));
        }
      }
      return List.copyOf(profiles);
    } catch (IOException exception) {
      throw new UncheckedIOException("cannot read scenario catalog", exception);
    }
  }

  public record Profile(String name, String yaml) { }
}
