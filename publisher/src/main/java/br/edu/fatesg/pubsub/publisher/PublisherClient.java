package br.edu.fatesg.pubsub.publisher;

import br.edu.fatesg.pubsub.common.Command;
import br.edu.fatesg.pubsub.common.Message;
import br.edu.fatesg.pubsub.common.Protocol;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.logging.Logger;

/** Cliente que publica mensagens no broker usando o protocolo compartilhado. */
public class PublisherClient implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(PublisherClient.class.getName());

    private final String host;
    private final int port;

    private Socket socket;
    private BufferedWriter writer;
    private boolean closed;

    public PublisherClient(String host, int port) {
        this.host = Objects.requireNonNull(host, "host não pode ser nulo");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host não pode ser vazio");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("porta deve estar entre 1 e 65535");
        }
        this.port = port;
    }

    /** Abre a conexão TCP com o broker. */
    public synchronized void connect() throws IOException {
        if (closed) {
            throw new IllegalStateException("cliente já foi encerrado");
        }
        if (socket != null) {
            throw new IllegalStateException("cliente já conectado");
        }

        socket = new Socket(host, port);
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        LOG.info(() -> "conectado ao broker " + host + ":" + port);
    }

    /** Envia uma mensagem PUB terminada por quebra de linha. */
    public synchronized void publish(String topic, String payload) throws IOException {
        if (socket == null || closed) {
            throw new IllegalStateException("cliente não está conectado");
        }

        writer.write(Protocol.encode(new Message(Command.PUB, topic, payload)));
        writer.write('\n');
        writer.flush();
    }

    /** Fecha a conexão. Pode ser chamado mais de uma vez. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (socket == null) {
            return;
        }
        try {
            socket.close();
            LOG.info("conexão com o broker encerrada");
        } catch (IOException e) {
            LOG.warning("erro ao fechar a conexão com o broker: " + e.getMessage());
        }
    }
}
