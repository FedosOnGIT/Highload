package nadutkin.database;

import java.nio.file.Path;

public record Config(
        Path basePath,
        long flushThresholdBytes) {
}
