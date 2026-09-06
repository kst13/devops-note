package dev.devopsnote.kafkarunner.sample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class SampleArgsTest {
    @Test void defaultWhenMissing() { assertThat(SampleArgs.count(List.of(), 10)).isEqualTo(10); }
    @Test void parsesFirstArg() { assertThat(SampleArgs.count(List.of("25"), 10)).isEqualTo(25); }
    @Test void rejectsNonNumber() {
        assertThatThrownBy(() -> SampleArgs.count(List.of("ten"), 10))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ten");
    }
    @Test void rejectsZeroOrNegative() {
        assertThatThrownBy(() -> SampleArgs.count(List.of("0"), 10)).isInstanceOf(IllegalArgumentException.class);
    }
}
