package ru.nsu.replicarecovery.runner.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ScenarioCatalogTest {
  @TempDir Path root;

  @Test
  void listsOnlyRegularYamlFilesInDeterministicOrderAndReadsUtf8() throws Exception {
    Files.writeString(root.resolve("z.yaml"), "name: пример", StandardCharsets.UTF_8);
    Files.writeString(root.resolve("a.yml"), "name: first", StandardCharsets.UTF_8);
    Files.writeString(root.resolve("load.json"), "{}");
    Files.createDirectory(root.resolve("directory.yaml"));
    Files.createSymbolicLink(root.resolve("linked.yaml"), root.resolve("a.yml"));

    var profiles = new ScenarioCatalog(root).profiles();
    assertThat(profiles).extracting(ScenarioCatalog.Profile::name).containsExactly("a.yml", "z.yaml");
    assertThat(profiles.get(1).yaml()).isEqualTo("name: пример");
  }

  @Test
  void absentDirectoryIsAnEmptyCatalogAndFileIsRejected() throws Exception {
    assertThat(new ScenarioCatalog(root.resolve("absent")).profiles()).isEmpty();
    Path file = Files.writeString(root.resolve("file"), "data");
    assertThatThrownBy(() -> new ScenarioCatalog(file).profiles()).isInstanceOf(RuntimeException.class);
  }
}
