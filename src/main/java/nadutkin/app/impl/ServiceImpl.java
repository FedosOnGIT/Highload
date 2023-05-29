package nadutkin.app.impl;

import jdk.incubator.foreign.MemorySegment;
import nadutkin.ServiceFactory;
import nadutkin.app.Service;
import nadutkin.app.server.HighLoadHttpServer;
import nadutkin.database.BaseEntry;
import nadutkin.database.Config;
import nadutkin.database.Entry;
import nadutkin.database.impl.MemorySegmentDao;
import nadutkin.utils.Constants;
import nadutkin.utils.ServiceConfig;
import one.nio.http.HttpServer;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static nadutkin.utils.UtilsClass.createConfigFromPort;

public class ServiceImpl implements Service {

    private final ServiceConfig config;
    private HttpServer server;
    private MemorySegmentDao dao;

    public ServiceImpl(ServiceConfig config) {
        this.config = config;
    }

    private byte[] getBytes(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public CompletableFuture<?> start() throws IOException {
        dao = new MemorySegmentDao(new Config(config.workingDir(), Constants.FLUSH_THRESHOLD_BYTES));
        server = new HighLoadHttpServer(createConfigFromPort(config.selfPort()));
        server.addRequestHandlers(this);
        server.start();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> stop() throws IOException {
        server.stop();
        dao.close();
        return CompletableFuture.completedFuture(null);
    }

    private MemorySegment getKey(String id) {
        return MemorySegment.ofArray(getBytes(id));
    }

    private Response upsert(String id, MemorySegment value, String goodResponse) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, getBytes("Id can not be null or empty!"));
        } else {
            MemorySegment key = getKey(id);
            Entry<MemorySegment> entry = new BaseEntry<>(key, value);
            dao.upsert(entry);
            return new Response(goodResponse, Response.EMPTY);
        }
    }

    @Path(Constants.REQUEST_PATH)
    public Response handleRequest(@Param(value = "id") String id,
                                  Request request) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, getBytes("Id can not be null or empty!"));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET -> {
                Entry<MemorySegment> value = dao.get(getKey(id));
                if (value == null) {
                    return new Response(Response.NOT_FOUND,
                            getBytes("Can't find any value, for id %1$s".formatted(id)));
                } else {
                    return new Response(Response.OK, value.value().toByteArray());
                }
            }
            case Request.METHOD_PUT -> {
                return upsert(id, MemorySegment.ofArray(request.getBody()), Response.CREATED);
            }
            case Request.METHOD_DELETE -> {
                return upsert(id, null, Response.ACCEPTED);
            }
            default -> {
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        }
    }

    @ServiceFactory(stage = 1, week = 1, bonuses = {"SingleNodeTest#respectFileFolder"})
    public static class Factory implements ServiceFactory.Factory {

        @Override
        public Service create(ServiceConfig config) {
            return new ServiceImpl(config);
        }
    }
}
