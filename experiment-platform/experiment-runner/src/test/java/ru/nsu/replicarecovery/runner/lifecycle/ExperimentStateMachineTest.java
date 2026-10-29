package ru.nsu.replicarecovery.runner.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static ru.nsu.replicarecovery.model.ExperimentState.*;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.nsu.replicarecovery.model.ExperimentState;

class ExperimentStateMachineTest {
  private final ExperimentStateMachine machine = new ExperimentStateMachine();

  @ParameterizedTest
  @MethodSource("validTransitions")
  void acceptsValidTransition(ExperimentState from, ExperimentState to) {
    assertThat(machine.transition(from, to)).isEqualTo(to);
  }

  static Stream<Arguments> validTransitions() {
    return Stream.of(Arguments.of(CREATED, VALIDATING), Arguments.of(VALIDATING, PREPARING),
        Arguments.of(PREPARING, WARMING_UP), Arguments.of(WARMING_UP, RUNNING),
        Arguments.of(RUNNING, STOPPING), Arguments.of(STOPPING, CLEANING_UP),
        Arguments.of(CLEANING_UP, COMPLETED), Arguments.of(CLEANING_UP, FAILED),
        Arguments.of(CLEANING_UP, CANCELLED), Arguments.of(CREATED, CLEANING_UP),
        Arguments.of(VALIDATING, CLEANING_UP), Arguments.of(PREPARING, CLEANING_UP),
        Arguments.of(WARMING_UP, CLEANING_UP), Arguments.of(RUNNING, CLEANING_UP),
        Arguments.of(CREATED, STOPPING), Arguments.of(VALIDATING, STOPPING),
        Arguments.of(PREPARING, STOPPING), Arguments.of(WARMING_UP, STOPPING));
  }

  @Test
  void rejectsSkippingCleanupAndLeavingTerminalStates() {
    assertThatThrownBy(() -> machine.transition(RUNNING, COMPLETED))
        .isInstanceOf(IllegalArgumentException.class);
    for (ExperimentState terminal : new ExperimentState[] {COMPLETED, FAILED, CANCELLED}) {
      for (ExperimentState next : ExperimentState.values()) {
        assertThatThrownBy(() -> machine.transition(terminal, next))
            .isInstanceOf(IllegalArgumentException.class);
      }
    }
  }
}
