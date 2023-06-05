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

    @Path(Constants.REQUEST_PATH)
    public Response handleRequest(@Param(value = "id") String id,
                                  Request request,
                                  @Param(value = "ack") Integer ack,
                                  @Param(value = "from") Integer from) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, getBytes("Id can not be null or empty!"));
        }
        int neighbours = from == null ? this.config.clusterUrls().size() : from;
        int quorum = ack == null ? neighbours / 2 + 1 : ack;

        if (quorum > neighbours || quorum <= 0) {
            return new Response(Response.BAD_REQUEST,
                    getBytes("ack and from - two positive ints, ack <= from"));
        }

        List<String> urls = sharder.getShardUrls(id, neighbours);

        ResponseProcessor processor = new ResponseProcessor(request.getMethod(), quorum);
        long timestamp = System.currentTimeMillis();
        try {
            byte[] body = request.getMethod() == Request.METHOD_PUT ? request.getBody() : null;
            request.setBody(UtilsClass.valueToSegment(new StoredValue(body, timestamp)));
        } catch (IOException e) {
            return new Response(Response.BAD_REQUEST,
                    getBytes("Can't ask other replicas, %s$".formatted(e.getMessage())));
        }
        for (String url : urls) {
            if (breaker.isWorking(url)) {
                Response response = url.equals(config.selfUrl())
                        ? handleV1(id, request)
                        : proxyRequest(url, request);
                if (processor.process(response)) {
                    break;
                }
            }
        }
        return processor.response();
    }

    private Response proxyRequest(String url, Request request) {
        try {
            HttpRequest proxyRequest = HttpRequest
                    .newBuilder(URI.create(url + request.getURI().replace(Constants.REQUEST_PATH, Constants.REPLICA_PATH)))
                    .method(
                            request.getMethodName(),
                            HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                    .build();
            HttpResponse<byte[]> response = client.send(proxyRequest, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                fail(url);
            } else {
                breaker.success(url);
            }
            return new Response(Integer.toString(response.statusCode()), response.body());
        } catch (InterruptedException | IOException exception) {
            Constants.LOG.error("Server caught an exception at url {}", url);
            fail(url);
        }
        return null;
    }

    @ServiceFactory(stage = 4, week = 4, bonuses = {"SingleNodeTest#respectFileFolder"})
    public static class Factory implements ServiceFactory.Factory {

        @Override
        public Service create(ServiceConfig config) {
            return new ServiceImpl(config);
        }
    }
}
