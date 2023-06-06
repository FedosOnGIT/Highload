package nadutkin.app.impl;

import nadutkin.ServiceFactory;
import nadutkin.app.Service;
import nadutkin.app.replicas.ReplicaService;
import nadutkin.app.replicas.ResponseProcessor;
import nadutkin.app.replicas.StoredValue;
import nadutkin.app.shards.CircuitBreaker;
import nadutkin.app.shards.JumpHashSharder;
import nadutkin.app.shards.Sharder;
import nadutkin.utils.Constants;
import nadutkin.utils.ServiceConfig;
import nadutkin.utils.UtilsClass;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ServiceImpl extends ReplicaService {
    private Sharder sharder;
    private HttpClient client;
    private CircuitBreaker breaker;

    public ServiceImpl(ServiceConfig config) {
        super(config);
    }

    private byte[] getBytes(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public CompletableFuture<?> start() throws IOException {
        this.sharder = new JumpHashSharder(config.clusterUrls());
        this.breaker = new CircuitBreaker(config.clusterUrls());
        this.client = HttpClient.newHttpClient();
        return super.start();
    }

    @Override
    public CompletableFuture<?> stop() throws IOException {
        server.stop();
        dao.close();
        return CompletableFuture.completedFuture(null);
    }

    private void fail(String url) {
        breaker.fail(url);
        Constants.LOG.error("Failed to request to shard {}", url);
    }

    private void processResponse(Response response,
                                 HttpSession session,
                                 ResponseProcessor processor) {
        if (processor.process(response)) {
            try {
                session.sendResponse(processor.response());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Path(Constants.REQUEST_PATH)
    public void handleRequest(@Param(value = "id") String id,
                              Request request,
                              @Param(value = "ack") Integer ack,
                              @Param(value = "from") Integer from,
                              HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, getBytes("Id can not be null or empty!")));
            return;
        }

        int neighbours = from == null ? this.config.clusterUrls().size() : from;
        int quorum = ack == null ? neighbours / 2 + 1 : ack;

        if (quorum > neighbours || quorum <= 0) {
            session.sendResponse(new Response(Response.BAD_REQUEST,
                    getBytes("ack and from - two positive ints, ack <= from")));
            return;
        }

        List<String> urls = sharder.getShardUrls(id, neighbours);
        ResponseProcessor processor = new ResponseProcessor(request.getMethod(), quorum, neighbours);
        long timestamp = System.currentTimeMillis();

        try {
            byte[] body = request.getMethod() == Request.METHOD_PUT ? request.getBody() : null;
            request.setBody(UtilsClass.valueToSegment(new StoredValue(body, timestamp)));
        } catch (IOException e) {
            session.sendResponse(new Response(Response.BAD_REQUEST,
                    getBytes("Can't ask other replicas, %s$".formatted(e.getMessage()))));
            return;
        }

        boolean visitDB = false;

        for (final String url : urls) {
            if (url.equals(config.selfUrl())) {
                visitDB = true;
            } else {
                proxyRequest(url, request)
                        .whenCompleteAsync((response, throwable) -> processResponse(response, session, processor));
            }
        }

        if (visitDB) {
            processResponse(handleV1(id, request), session, processor);
        }
    }

    private CompletableFuture<Response> proxyRequest(String url, Request request) {
        HttpRequest proxyRequest = HttpRequest
                .newBuilder(URI.create(url + request.getURI().replace(Constants.REQUEST_PATH, Constants.REPLICA_PATH)))
                .method(
                        request.getMethodName(),
                        HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                .build();
        return client.sendAsync(proxyRequest, HttpResponse.BodyHandlers.ofByteArray())
                .handleAsync((response, exception) -> {
                    if (exception != null || response.statusCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
                        fail(url);
                        Constants.LOG.error("Server caught an exception at url {}", url);
                        return null;
                    }
                    breaker.success(url);
                    return new Response(Integer.toString(response.statusCode()), response.body());
                });
    }

    @ServiceFactory(stage = 6, week = 6, bonuses = {"SingleNodeTest#respectFileFolder"})
    public static class Factory implements ServiceFactory.Factory {

        @Override
        public Service create(ServiceConfig config) {
            return new ServiceImpl(config);
        }
    }
}
