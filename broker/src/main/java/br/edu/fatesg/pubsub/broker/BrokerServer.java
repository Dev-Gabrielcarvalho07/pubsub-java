package br.edu.fatesg.pubsub.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servidor TCP do broker: aceita conexões e atende cada cliente em uma thread do pool.
 */
public class BrokerServer {

    private static final Logger LOG = Logger.getLogger(BrokerServer.class.getName());

    private final int port;
    private final Consumer<Socket> clientHandler;
    private final ExecutorService pool;
    // [APRESENTAÇÃO] O stop() fecha estes sockets porque interromper a thread não destrava um readLine() bloqueado.
    private final Set<Socket> clientesConectados = ConcurrentHashMap.newKeySet();

    private volatile ServerSocket serverSocket;
    private volatile boolean running;

    /** Cria o servidor na porta informada (0 = porta livre escolhida pelo sistema). */
    public BrokerServer(int port, Consumer<Socket> clientHandler) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Porta inválida: " + port);
        }
        if (clientHandler == null) {
            throw new IllegalArgumentException("clientHandler não pode ser nulo");
        }
        this.port = port;
        this.clientHandler = clientHandler;
        // [APRESENTAÇÃO] Uma thread por cliente: o readLine() bloqueante de um cliente não trava o accept() nem os outros.
        this.pool = Executors.newCachedThreadPool();
    }

    /** Abre o ServerSocket e aceita conexões até {@link #stop()}; bloqueia a thread que o chamou. */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        LOG.info("Broker escutando na porta " + serverSocket.getLocalPort());

        // Checa também isClosed(): se o stop() rodar antes do "running = true" acima,
        // o socket já estará fechado e o loop termina em vez de girar para sempre.
        while (running && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                LOG.info("Cliente conectado: " + client.getRemoteSocketAddress());
                clientesConectados.add(client);
                // Se o stop() rodou entre o accept() e o add(), ele já não vê este socket
                if (!running) {
                    clientesConectados.remove(client);
                    fecharSemErro(client);
                    break;
                }
                pool.submit(() -> atender(client));
            } catch (RejectedExecutionException e) {
                fecharClientesConectados();
            } catch (SocketException e) {
                if (running && !serverSocket.isClosed()) {
                    LOG.log(Level.SEVERE, "Erro no ServerSocket", e);
                }
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Erro ao aceitar conexão", e);
            }
        }
    }

    /** Para de aceitar conexões, fecha o ServerSocket e os sockets dos clientes e encerra o pool. */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Erro ao fechar o ServerSocket", e);
        }
        fecharClientesConectados();
        pool.shutdownNow();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Broker encerrado");
    }

    /** Porta em que o broker está escutando de fato (útil quando foi iniciado com porta 0). */
    public int getLocalPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : -1;
    }

    public boolean isRunning() {
        return running;
    }

    private void atender(Socket client) {
        try (client) {
            clientHandler.accept(client);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Erro ao atender cliente " + client.getRemoteSocketAddress(), e);
        } finally {
            clientesConectados.remove(client);
        }
    }

    private void fecharClientesConectados() {
        for (Socket client : clientesConectados) {
            fecharSemErro(client);
        }
    }

    private static void fecharSemErro(Socket client) {
        try {
            client.close();
        } catch (IOException e) {
            // ignorado: o cliente já está sendo desconectado
        }
    }
}
