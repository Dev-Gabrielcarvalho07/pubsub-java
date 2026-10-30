package br.edu.fatesg.pubsub.broker;

import br.edu.fatesg.pubsub.common.Message;
import br.edu.fatesg.pubsub.common.Protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Atende UM cliente conectado ao broker (publisher, subscriber ou os dois).
 *
 * <p>{@link #run()} lê os comandos do cliente e repassa ao {@link TopicRegistry}. Como o handler também é um
 * {@link Subscriber}, é ele mesmo que se inscreve nos tópicos e recebe, via {@link #send(Message)}, as mensagens
 * MSG que o registry entrega.</p>
 *
 * <p>Quem fecha o socket é o {@link BrokerServer}, quando {@code run()} termina.</p>
 */
public class ClientHandler implements Subscriber, Runnable {

    private static final Logger LOG = Logger.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final TopicRegistry registry;
    private final BufferedWriter out;
    private final String remoteAddress;

    /**
     * @throws UncheckedIOException se não for possível abrir o canal de saída do socket
     */
    public ClientHandler(Socket socket, TopicRegistry registry) {
        if (socket == null) {
            throw new IllegalArgumentException("socket não pode ser nulo");
        }
        if (registry == null) {
            throw new IllegalArgumentException("registry não pode ser nulo");
        }
        this.socket = socket;
        this.registry = registry;
        this.remoteAddress = String.valueOf(socket.getRemoteSocketAddress());
        try {
            this.out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Não foi possível abrir a saída do cliente " + remoteAddress, e);
        }
    }

    /**
     * Lê os comandos do cliente até ele desconectar. Ao terminar, sempre remove o cliente de todos os tópicos.
     */
    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                handleLine(line);
            }
        } catch (IOException e) {
            // cliente caiu sem fechar a conexão; tratado como desconexão
            LOG.log(Level.FINE, "Erro de rede com o cliente " + this, e);
        } finally {
            registry.removeFromAll(this);
            LOG.info("Cliente desconectado: " + this);
        }
    }

    private void handleLine(String line) {
        Message message;
        try {
            message = Protocol.decode(line);
        } catch (IllegalArgumentException e) {
            LOG.warning("[" + this + "] Linha inválida ignorada: " + e.getMessage());
            return;
        }

        try {
            switch (message.command()) {
                case SUB -> registry.subscribe(message.topic(), this);
                case UNSUB -> registry.unsubscribe(message.topic(), this);
                case PUB -> registry.publish(message.topic(), message.payload());
                // MSG é só do broker para o subscriber; cliente não pode enviar
                case MSG -> LOG.warning("[" + this + "] Comando MSG recusado: só o broker envia MSG");
            }
        } catch (IllegalArgumentException e) {
            LOG.warning("[" + this + "] Comando recusado: " + e.getMessage());
        }
    }

    /**
     * Envia a mensagem ao cliente. Sincronizado para que publicações simultâneas, vindas de threads de outros
     * clientes, não misturem as linhas.
     *
     * @throws UncheckedIOException se a escrita falhar (o {@link TopicRegistry} trata isso por inscrito)
     */
    @Override
    public synchronized void send(Message message) {
        try {
            out.write(Protocol.encode(message));
            out.write('\n');
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Falha ao enviar mensagem ao cliente " + this, e);
        }
    }

    /** Endereço remoto do cliente, para identificar o cliente nos logs. */
    @Override
    public String toString() {
        return remoteAddress;
    }
}
