package nadutkin.app.server;

import nadutkin.app.range.ChunkResponse;
import nadutkin.app.range.RangeQueueItem;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;
import one.nio.net.Socket;
import one.nio.server.SelectorThread;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static nadutkin.database.impl.Constants.LOG;
import static nadutkin.utils.UtilsClass.getBytes;
import static nadutkin.utils.UtilsClass.shutdownAndAwaitTermination;

public class HighLoadHttpServer extends HttpServer {
    private final ExecutorService executors;

    public HighLoadHttpServer(HttpServerConfig config, Object... routers) throws IOException {
        super(config, routers);
        final int maximumPoolSize = Runtime.getRuntime().availableProcessors();
        final int corePoolSize = Math.max(1, maximumPoolSize / 2);
        final long keepAliveTime = 1;
        executors = new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new BlockingStack<>());
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, getBytes("Incorrect request path"));
        session.sendResponse(response);
    }

    @Override
    public synchronized void stop() {
        shutdownAndAwaitTermination(executors);
        for (SelectorThread selector : selectors) {
            if (selector.selector.isOpen()) {
                for (Session session : selector.selector) {
                    session.close();
                }
            }
        }
        super.stop();
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        executors.execute(() -> {
            try {
                super.handleRequest(request, session);
            } catch (Exception e) {
                LOG.error("Caught an exception while trying to handle request. Exception: {}", e.getMessage());
                try {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } catch (IOException ex) {
                    LOG.error("Unable to send bad request. Exception: {}", ex.getMessage());
                }
            }
        });
    }

    @Override
    public HttpSession createSession(Socket socket) {
        return new HttpSession(socket, this) {
            @Override
            public synchronized void sendResponse(Response response) throws IOException {
                if (response instanceof ChunkResponse) {
                    super.write(new RangeQueueItem(response.getBody()));
                } else {
                    super.sendResponse(response);
                }
            }
        };
    }
}
