package ru.nsu.replicarecovery.runner.web;

import java.util.regex.Pattern;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;
import ru.nsu.replicarecovery.runner.domain.ExperimentRecord;
import ru.nsu.replicarecovery.runner.lifecycle.ExperimentCoordinator;

/** Pages are views of the same REST API used by the CLI scripts. */
@Controller
public class PageController {
  private static final Pattern VALID_ID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}");
  private final ExperimentCoordinator coordinator;
  private final Environment environment;

  public PageController(ExperimentCoordinator coordinator, Environment environment) {
    this.coordinator = coordinator;
    this.environment = environment;
  }

  @ModelAttribute("simulated")
  boolean simulated() {
    return environment.matchesProfiles("test-lab");
  }

  @GetMapping("/")
  String index() {
    return "index";
  }

  @GetMapping("/experiments/{id}")
  String active(@PathVariable String id, Model model) {
    existing(id);
    model.addAttribute("experimentId", id);
    return "active";
  }

  @GetMapping("/experiments/{id}/results")
  String results(@PathVariable String id, Model model) {
    if (!existing(id).terminal()) {
      return "redirect:/experiments/" + id;
    }
    model.addAttribute("experimentId", id);
    return "results";
  }

  private ExperimentRecord existing(String id) {
    if (!VALID_ID.matcher(id).matches()) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid experiment ID");
    }
    try {
      return coordinator.get(id);
    } catch (ExperimentCoordinator.UnknownExperimentException exception) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Experiment not found");
    }
  }
}
