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
 * Atende um cliente: repassa seus comandos ao {@link TopicRegistry} e, como {@link Subscriber}, recebe as MSG dele.
 */
public class ClientHandler implements Subscriber, Runnable {

    private static final Logger LOG = Logger.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final TopicRegistry registry;
    private final BufferedWriter out;
    private final String remoteAddress;

    /** Prepara a saída do socket; lança UncheckedIOException se não conseguir abri-la. */
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

    /** Lê os comandos do cliente até ele desconectar. */
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
            LOG.log(Level.FINE, "Erro de rede com o cliente " + this, e);
        } finally {
            // [APRESENTAÇÃO] Se o cliente cai, readLine() devolve null ou lança IOException e ele sai de todos os tópicos aqui.
            registry.removeFromAll(this);
            LOG.info("Cliente desconectado: " + this);
        }
    }

    /** Escreve a mensagem no socket do cliente; falhas de escrita viram UncheckedIOException. */
    // [APRESENTAÇÃO] synchronized: vários publishers, cada um na sua thread, podem chamar send() deste cliente ao mesmo tempo sem misturar as linhas.
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

    /** Endereço remoto do cliente, usado para identificá-lo nos logs. */
    @Override
    public String toString() {
        return remoteAddress;
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
                case MSG -> LOG.warning("[" + this + "] Comando MSG recusado: só o broker envia MSG");
            }
        } catch (IllegalArgumentException e) {
            LOG.warning("[" + this + "] Comando recusado: " + e.getMessage());
        }
    }
}
