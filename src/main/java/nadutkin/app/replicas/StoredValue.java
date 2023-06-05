package nadutkin.app.replicas;

import java.io.Serializable;

public record StoredValue(byte[] value, Long timestamp) implements Serializable {
}
