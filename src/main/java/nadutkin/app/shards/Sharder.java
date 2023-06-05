package nadutkin.app.shards;

import java.util.List;

public abstract class Sharder {
    protected final List<String> shards;

    public Sharder(List<String> shards) {
        this.shards = shards;
    }

    public abstract Integer getShard(String key);

    public abstract List<String> getShardUrls(String key, int from);
}
