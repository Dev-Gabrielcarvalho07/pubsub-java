package br.edu.fatesg.pubsub.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BrokerServerTest {

    private BrokerServer server;
    private final List<Socket> clientes = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (Socket s : clientes) {
            s.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    /** Sobe o servidor numa thread própria, em porta livre, e espera ele começar a escutar. */
    private void iniciar(BrokerServer s) throws InterruptedException {
        server = s;
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        t.setDaemon(true);
        t.start();
        long limite = System.currentTimeMillis() + 5000;
        while (server.getLocalPort() <= 0 && System.currentTimeMillis() < limite) {
            Thread.sleep(10);
        }
        assertTrue(server.getLocalPort() > 0, "servidor não começou a escutar");
    }

    @Test
    void atendeVariosClientesAoMesmoTempo() throws Exception {
        int total = 5;
        CountDownLatch todosConectados = new CountDownLatch(total);
        CountDownLatch liberar = new CountDownLatch(1);

        // Cada handler fica "preso" até todos os clientes chegarem.
        // Se o servidor atendesse um por vez, o latch nunca chegaria a zero.
        iniciar(new BrokerServer(0, socket -> {
            todosConectados.countDown();
            try {
                liberar.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        for (int i = 0; i < total; i++) {
            clientes.add(new Socket("localhost", server.getLocalPort()));
        }

        assertTrue(todosConectados.await(5, TimeUnit.SECONDS),
                "nem todos os clientes foram atendidos em paralelo");
        liberar.countDown();
    }

    @Test
    void erroEmUmClienteNaoDerrubaOBroker() throws Exception {
        CountDownLatch atendidos = new CountDownLatch(2);
        iniciar(new BrokerServer(0, socket -> {
            atendidos.countDown();
            throw new RuntimeException("falha simulada");
        }));

        // O primeiro handler lança exceção; o segundo cliente ainda precisa ser atendido
        clientes.add(new Socket("localhost", server.getLocalPort()));
        long limite = System.currentTimeMillis() + 5000;
        while (atendidos.getCount() == 2 && System.currentTimeMillis() < limite) {
            Thread.sleep(10);
        }
        clientes.add(new Socket("localhost", server.getLocalPort()));

        assertTrue(atendidos.await(5, TimeUnit.SECONDS));
        assertTrue(server.isRunning());
    }

    @Test
    void rejeitaPortaInvalida() {
        assertThrows(IllegalArgumentException.class, () -> new BrokerServer(70000, s -> { }));
        assertThrows(IllegalArgumentException.class, () -> new BrokerServer(-1, s -> { }));
    }

    @Test
    void resolvePortaPorArgumentoOuPadrao() {
        assertEquals(6000, BrokerMain.resolvePort(new String[] {"6000"}));
        if (System.getenv("BROKER_PORT") == null) {
            assertEquals(BrokerMain.DEFAULT_PORT, BrokerMain.resolvePort(new String[0]));
        }
    }
}
