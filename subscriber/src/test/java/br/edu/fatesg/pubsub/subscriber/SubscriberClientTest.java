package br.edu.fatesg.pubsub.subscriber;

import br.edu.fatesg.pubsub.common.Command;
import br.edu.fatesg.pubsub.common.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Testa o {@link SubscriberClient} contra um broker falso: um {@link ServerSocket} local na porta 0.
 */
class SubscriberClientTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private ServerSocket server;
    private Socket brokerSide;
    private BufferedReader fromClient;
    private BufferedWriter toClient;

    private final BlockingQueue<Message> received = new LinkedBlockingQueue<>();
    private SubscriberClient client;

    @BeforeEach
    void connectClientToFakeBroker() throws IOException {
        server = new ServerSocket(0);
        server.setSoTimeout((int) TIMEOUT.toMillis());

        client = new SubscriberClient("localhost", server.getLocalPort(), received::add);
        client.connect();

        brokerSide = server.accept();
        brokerSide.setSoTimeout((int) TIMEOUT.toMillis());
        fromClient = new BufferedReader(new InputStreamReader(brokerSide.getInputStream(), StandardCharsets.UTF_8));
        toClient = new BufferedWriter(new OutputStreamWriter(brokerSide.getOutputStream(), StandardCharsets.UTF_8));
    }

    @AfterEach
    void closeEverything() throws IOException {
        client.close();
        brokerSide.close();
        server.close();
    }

    @Test
    void subscribeEnviaLinhaSub() throws IOException {
        client.subscribe("esportes");

        assertEquals("SUB|esportes", fromClient.readLine());
    }

    @Test
    void unsubscribeEnviaLinhaUnsub() throws IOException {
        client.unsubscribe("esportes");

        assertEquals("UNSUB|esportes", fromClient.readLine());
    }

    @Test
    void subscribeEnviaTopicoEmUtf8() throws IOException {
        client.subscribe("notícias");

        assertEquals("SUB|notícias", fromClient.readLine());
    }

    @Test
    void mensagemMsgDoServidorChegaAoConsumer() throws Exception {
        sendToClient("MSG|esportes|gol do time A");

        assertEquals(new Message(Command.MSG, "esportes", "gol do time A"), nextReceived());
    }

    @Test
    void conteudoComSeparadorEAcentosChegaIntactoAoConsumer() throws Exception {
        sendToClient("MSG|notícias|placar 2|1 — ação");

        assertEquals("placar 2|1 — ação", nextReceived().payload());
    }

    @Test
    void linhasQueNaoSaoMsgSaoIgnoradasEALeituraContinua() throws Exception {
        sendToClient("PUB|esportes|não deveria chegar");
        sendToClient("SUB|esportes");
        sendToClient("UNSUB|esportes");
        sendToClient("MSG|esportes|depois das ignoradas");

        // as linhas são processadas em ordem: se a MSG chegou, as anteriores já foram descartadas
        assertEquals("depois das ignoradas", nextReceived().payload());
        assertTrue(received.isEmpty(), "nenhuma outra mensagem deveria ter chegado: " + received);
    }

    @Test
    void linhasInvalidasSaoIgnoradasEALeituraContinua() throws Exception {
        sendToClient("lixo");
        sendToClient("msg|esportes|minúsculo");
        sendToClient("MSG|esportes");
        sendToClient("");
        sendToClient("MSG|esportes|depois das inválidas");

        assertEquals("depois das inválidas", nextReceived().payload());
        assertTrue(received.isEmpty(), "nenhuma outra mensagem deveria ter chegado: " + received);
        assertTrue(client.isConnected());
    }

    @Test
    void erroNoConsumerNaoInterrompeALeitura() throws Exception {
        BlockingQueue<Message> ok = new LinkedBlockingQueue<>();
        try (ServerSocket otherServer = new ServerSocket(0);
             SubscriberClient failingClient = new SubscriberClient("localhost", otherServer.getLocalPort(), msg -> {
                 if (msg.payload().equals("falha")) {
                     throw new RuntimeException("erro proposital no consumer");
                 }
                 ok.add(msg);
             })) {
            otherServer.setSoTimeout((int) TIMEOUT.toMillis());
            failingClient.connect();
            try (Socket broker = otherServer.accept();
                 BufferedWriter out = new BufferedWriter(
                         new OutputStreamWriter(broker.getOutputStream(), StandardCharsets.UTF_8))) {
                out.write("MSG|t|falha\nMSG|t|ok\n");
                out.flush();

                Message msg = ok.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertNotNull(msg, "a mensagem seguinte ao erro não chegou");
                assertEquals("ok", msg.payload());
            }
        }
    }

    @Test
    void isConnectedEhTrueAposConnect() {
        assertTrue(client.isConnected());
    }

    @Test
    void quandoServidorFechaConexaoIsConnectedViraFalse() throws Exception {
        brokerSide.close();

        awaitUntil(() -> !client.isConnected(), "isConnected() não virou false após o servidor fechar");
    }

    @Test
    void quandoServidorFechaConexaoSubscribeLancaIllegalStateException() throws Exception {
        brokerSide.close();
        awaitUntil(() -> !client.isConnected(), "isConnected() não virou false após o servidor fechar");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> client.subscribe("esportes"));
        assertEquals("conexão com o broker perdida", e.getMessage());
        assertThrows(IllegalStateException.class, () -> client.unsubscribe("esportes"));
    }

    @Test
    void closeEncerraSemErroEPodeSerChamadoDuasVezes() throws IOException {
        assertDoesNotThrow(client::close);
        assertDoesNotThrow(client::close);

        assertFalse(client.isConnected());
        // do lado do broker, o fim da conexão aparece como fim do stream
        assertNull(fromClient.readLine());
    }

    @Test
    void subscribeAposCloseLancaIllegalStateException() {
        client.close();

        assertThrows(IllegalStateException.class, () -> client.subscribe("esportes"));
    }

    @Test
    void connectAposCloseIndicaQueEhPrecisoCriarNovoCliente() {
        client.close();

        IllegalStateException e = assertThrows(IllegalStateException.class, client::connect);
        assertEquals("conexão encerrada; crie um novo SubscriberClient para reconectar", e.getMessage());
    }

    private void sendToClient(String line) throws IOException {
        toClient.write(line);
        toClient.write('\n');
        toClient.flush();
    }

    private Message nextReceived() throws InterruptedException {
        Message msg = received.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (msg == null) {
            fail("nenhuma mensagem chegou ao Consumer em " + TIMEOUT.toSeconds() + "s");
        }
        return msg;
    }

    /**
     * Espera até a condição ficar verdadeira ou o {@link #TIMEOUT} estourar. O estado de conexão não tem evento
     * para aguardar, então a verificação é repetida em intervalos curtos até o prazo.
     */
    private static void awaitUntil(BooleanSupplier condition, String failureMessage) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail(failureMessage);
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }
}
