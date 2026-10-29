package br.edu.fatesg.pubsub.subscriber;

import br.edu.fatesg.pubsub.common.Command;
import br.edu.fatesg.pubsub.common.Message;
import br.edu.fatesg.pubsub.common.Protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cliente subscriber: conecta ao broker, assina tópicos e repassa as mensagens MSG recebidas a um {@link Consumer}.
 *
 * <p>A leitura acontece em uma thread separada, iniciada em {@link #connect()}. O {@code onMessage} é chamado
 * nessa thread.</p>
 */
public class SubscriberClient implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SubscriberClient.class.getName());

    private static final long READER_JOIN_TIMEOUT_MS = 2_000;

    private final String host;
    private final int port;
    private final Consumer<Message> onMessage;

    private Socket socket;
    private BufferedWriter writer;
    private Thread readerThread;
    private volatile boolean connected;
    private volatile boolean closing;

    public SubscriberClient(String host, int port, Consumer<Message> onMessage) {
        this.host = Objects.requireNonNull(host, "host não pode ser nulo");
        this.port = port;
        this.onMessage = Objects.requireNonNull(onMessage, "onMessage não pode ser nulo");
    }

    /**
     * Abre a conexão com o broker e inicia a thread de leitura.
     */
    public synchronized void connect() throws IOException {
        if (socket != null) {
            if (isConnected()) {
                throw new IllegalStateException("cliente já conectado");
            }
            throw new IllegalStateException("conexão encerrada; crie um novo SubscriberClient para reconectar");
        }

        Socket s = new Socket(host, port);
        BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
        writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
        socket = s;
        connected = true;

        readerThread = new Thread(() -> readLoop(reader), "subscriber-reader");
        readerThread.setDaemon(true);
        readerThread.start();
        LOG.info(() -> "conectado ao broker " + host + ":" + port);
    }

    /**
     * Indica se a conexão com o broker está ativa. Fica {@code false} após {@link #close()} ou quando o broker
     * encerra/perde a conexão.
     */
    public boolean isConnected() {
        return connected && !closing;
    }

    public void subscribe(String topic) throws IOException {
        send(new Message(Command.SUB, topic, null));
    }

    public void unsubscribe(String topic) throws IOException {
        send(new Message(Command.UNSUB, topic, null));
    }

    /**
     * Encerra a conexão e aguarda a thread de leitura terminar. Pode ser chamado mais de uma vez.
     */
    @Override
    public void close() {
        Thread reader;
        synchronized (this) {
            if (socket == null || closing) {
                return;
            }
            closing = true;
            // fechar o socket desbloqueia o readLine() da thread de leitura
            closeSocketQuietly();
            reader = readerThread;
        }

        if (reader != null && reader != Thread.currentThread()) {
            try {
                reader.join(READER_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("conexão com o broker encerrada");
    }

    private synchronized void send(Message msg) throws IOException {
        if (socket == null || closing) {
            throw new IllegalStateException("cliente não está conectado");
        }
        if (!connected) {
            throw new IllegalStateException("conexão com o broker perdida");
        }
        writer.write(Protocol.encode(msg));
        writer.write('\n');
        writer.flush();
    }

    private void readLoop(BufferedReader reader) {
        try (reader) {
            String line;
            while ((line = reader.readLine()) != null) {
                handleLine(line);
            }
            if (!closing) {
                LOG.warning("broker encerrou a conexão");
            }
        } catch (IOException e) {
            if (!closing) {
                LOG.log(Level.WARNING, "conexão com o broker perdida: " + e.getMessage(), e);
            }
        } finally {
            connected = false;
            if (!closing) {
                closeSocketQuietly();
            }
        }
    }

    private void closeSocketQuietly() {
        try {
            socket.close();
        } catch (IOException e) {
            LOG.log(Level.FINE, "erro ao fechar o socket", e);
        }
    }

    private void handleLine(String line) {
        Message msg;
        try {
            msg = Protocol.decode(line);
        } catch (IllegalArgumentException e) {
            LOG.warning(() -> "linha inválida recebida do broker, ignorada: " + e.getMessage());
            return;
        }

        if (msg.command() != Command.MSG) {
            LOG.fine(() -> "mensagem ignorada (não é MSG): " + line);
            return;
        }

        try {
            onMessage.accept(msg);
        } catch (RuntimeException e) {
            // um erro no consumidor não deve derrubar a thread de leitura
            LOG.log(Level.WARNING, "erro ao processar mensagem do tópico " + msg.topic(), e);
        }
    }
}
