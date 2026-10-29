package br.edu.fatesg.pubsub.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BrokerServerTest {

    private BrokerServer server;
    private Thread threadDoServidor;
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
        threadDoServidor = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        threadDoServidor.setDaemon(true);
        threadDoServidor.start();
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
            conectar();
        }

        assertTrue(todosConectados.await(5, TimeUnit.SECONDS),
                "nem todos os clientes foram atendidos em paralelo");
        liberar.countDown();
    }

    @Test
    void erroEmUmClienteFechaSoAqueleSocketENaoDerrubaOBroker() throws Exception {
        CountDownLatch atendidos = new CountDownLatch(2);
        iniciar(new BrokerServer(0, socket -> {
            atendidos.countDown();
            throw new RuntimeException("falha simulada");
        }));

        // 1º cliente: o handler lança exceção e o broker deve fechar o socket dele
        Socket primeiro = conectar();
        primeiro.setSoTimeout(5000);
        assertEquals(-1, primeiro.getInputStream().read(), "socket do cliente com erro não foi fechado");

        // 2º cliente: o broker continua aceitando conexões normalmente
        conectar();
        assertTrue(atendidos.await(5, TimeUnit.SECONDS), "broker parou de atender após o erro");
        assertTrue(server.isRunning());
    }

    @Test
    void stopEncerraOLoopDoServidor() throws Exception {
        iniciar(new BrokerServer(0, socket -> { }));

        server.stop();

        // start() deve sair do loop de accept() e a thread do servidor terminar
        threadDoServidor.join(5000);
        assertFalse(threadDoServidor.isAlive(), "start() não retornou após stop()");
        assertFalse(server.isRunning());
    }

    @Test
    void rejeitaPortaInvalida() {
        assertThrows(IllegalArgumentException.class, () -> new BrokerServer(70000, s -> { }));
        assertThrows(IllegalArgumentException.class, () -> new BrokerServer(-1, s -> { }));
        assertThrows(IllegalArgumentException.class, () -> new BrokerServer(5000, null));
    }

    @Test
    void resolvePortaPorArgumentoOuPadrao() {
        assertEquals(6000, BrokerMain.resolvePort(new String[] {"6000"}));
        assertEquals(6000, BrokerMain.resolvePort(new String[] {" 6000 "}));
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.resolvePort(new String[] {"abc"}));
        if (System.getenv("BROKER_PORT") == null) {
            assertEquals(BrokerMain.DEFAULT_PORT, BrokerMain.resolvePort(new String[0]));
        }
    }

    private Socket conectar() throws IOException {
        Socket s = new Socket("localhost", server.getLocalPort());
        clientes.add(s);
        return s;
    }
}
