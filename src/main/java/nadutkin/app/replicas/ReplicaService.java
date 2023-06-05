package nadutkin.app.replicas;

import jdk.incubator.foreign.MemorySegment;
import nadutkin.app.Service;
import nadutkin.app.server.HighLoadHttpServer;
import nadutkin.database.BaseEntry;
import nadutkin.database.Config;
import nadutkin.database.Entry;
import nadutkin.database.impl.MemorySegmentDao;
import nadutkin.utils.Constants;
import nadutkin.utils.ServiceConfig;
import nadutkin.utils.UtilsClass;
import one.nio.http.HttpServer;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static nadutkin.utils.UtilsClass.getBytes;
import static nadutkin.utils.UtilsClass.getKey;

public class ReplicaService implements Service {
    protected final ServiceConfig config;
    protected final AtomicInteger storedData = new AtomicInteger(0);
    protected HttpServer server;
    protected MemorySegmentDao dao;

    public ReplicaService(ServiceConfig config) {
        this.config = config;
    }

    @Override
    public CompletableFuture<?> start() throws IOException {
        this.dao = new MemorySegmentDao(new Config(config.workingDir(), Constants.FLUSH_THRESHOLD_BYTES));
        this.server = new HighLoadHttpServer(UtilsClass.createConfigFromPort(config.selfPort()));
        server.addRequestHandlers(this);
        server.start();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> stop() throws IOException {
        this.server.stop();
        this.dao.close();
        return CompletableFuture.completedFuture(null);
    }

    private Response upsert(MemorySegment key, @Nonnull byte[] body, String goodResponse) {
        MemorySegment value = MemorySegment.ofArray(body);
        Entry<MemorySegment> entry = new BaseEntry<>(key, value);
        dao.upsert(entry);
        return new Response(goodResponse, Response.EMPTY);
    }

    @Path(Constants.REPLICA_PATH)
    public Response handleV1(@Param(value = "id", required = true) String id,
                             Request request) {
        MemorySegment key = getKey(id);
        switch (request.getMethod()) {
            case Request.METHOD_GET -> {
                Entry<MemorySegment> value = dao.get(key);
                if (value == null) {
                    return new Response(Response.NOT_FOUND,
                            getBytes("Can't find any value, for id %1$s".formatted(id)));
                } else {
                    return new Response(Response.OK, value.value().toByteArray());
                }
            }
            case Request.METHOD_PUT -> {
                storedData.getAndIncrement();
                return upsert(key, request.getBody(), Response.CREATED);
            }
            case Request.METHOD_DELETE -> {
                return upsert(key, request.getBody(), Response.ACCEPTED);
            }
            default -> {
                return new Response(Response.METHOD_NOT_ALLOWED,
                        getBytes("Not implemented yet"));
            }
        }
    }

}
