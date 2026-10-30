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
 * Servidor TCP do broker.
 *
 * <p>Abre um {@link ServerSocket} na porta informada e fica aceitando conexões
 * de publishers e subscribers. Cada cliente aceito é atendido em uma thread
 * separada de um {@link ExecutorService}, assim o laço de {@code accept()}
 * nunca fica bloqueado esperando um cliente terminar.</p>
 *
 * <p>O que fazer com cada conexão é definido pelo {@code clientHandler}
 * recebido no construtor. Na issue #3 ele passa a ser o {@code ClientHandler}
 * que interpreta os comandos SUB/UNSUB/PUB.</p>
 */
public class BrokerServer {

    private static final Logger LOG = Logger.getLogger(BrokerServer.class.getName());

    private final int port;
    private final Consumer<Socket> clientHandler;
    private final ExecutorService pool;
    // Sockets dos clientes em atendimento: o stop() fecha todos para destravar
    // as threads paradas no readLine() (interromper a thread não basta).
    private final Set<Socket> clientesConectados = ConcurrentHashMap.newKeySet();

    private volatile ServerSocket serverSocket;
    private volatile boolean running;

    /**
     * @param port          porta TCP onde o broker vai escutar (0 = porta livre escolhida pelo sistema)
     * @param clientHandler ação executada, em uma thread do pool, para cada cliente conectado
     */
    public BrokerServer(int port, Consumer<Socket> clientHandler) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Porta inválida: " + port);
        }
        if (clientHandler == null) {
            throw new IllegalArgumentException("clientHandler não pode ser nulo");
        }
        this.port = port;
        this.clientHandler = clientHandler;
        // Pool que cria threads sob demanda e reaproveita as ociosas:
        // um cliente lento nunca impede outro de ser atendido.
        this.pool = Executors.newCachedThreadPool();
    }

    /**
     * Abre o ServerSocket e fica aceitando conexões até {@link #stop()} ser chamado.
     * Este método bloqueia a thread que o chamou.
     */
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
                // pool já encerrado pelo stop(); o laço termina na próxima verificação
                fecharClientesConectados();
            } catch (SocketException e) {
                // accept() lança SocketException quando o ServerSocket é fechado pelo stop()
                if (running && !serverSocket.isClosed()) {
                    LOG.log(Level.SEVERE, "Erro no ServerSocket", e);
                }
            } catch (IOException e) {
                // Falha ao aceitar UM cliente não derruba o broker
                LOG.log(Level.WARNING, "Erro ao aceitar conexão", e);
            }
        }
    }

    /** Executa o handler e garante que o socket do cliente seja fechado no final. */
    private void atender(Socket client) {
        try (client) {
            clientHandler.accept(client);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Erro ao atender cliente " + client.getRemoteSocketAddress(), e);
        } finally {
            clientesConectados.remove(client);
        }
    }

    /**
     * Para de aceitar conexões, fecha o ServerSocket, desconecta os clientes ainda
     * conectados e encerra o pool de threads.
     */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Erro ao fechar o ServerSocket", e);
        }
        // Fechar o socket faz o readLine() do handler lançar exceção e a thread terminar
        fecharClientesConectados();
        pool.shutdownNow();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Broker encerrado");
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

    /** Porta em que o broker está escutando de fato (útil quando foi iniciado com porta 0). */
    public int getLocalPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : -1;
    }

    public boolean isRunning() {
        return running;
    }
}
