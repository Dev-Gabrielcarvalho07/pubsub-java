package br.edu.fatesg.pubsub.publisher;

import br.edu.fatesg.pubsub.common.Command;
import br.edu.fatesg.pubsub.common.Protocol;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublisherClientTest {

    @Test
    void publicaMensagemNoFormatoDoProtocoloEFechaConexao() throws Exception {
        try (ServerSocket brokerFalso = new ServerSocket(0)) {
            brokerFalso.setSoTimeout(3_000);
            CompletableFuture<String> linhaRecebida = CompletableFuture.supplyAsync(() -> {
                try (Socket cliente = brokerFalso.accept();
                     BufferedReader reader = new BufferedReader(
                             new InputStreamReader(cliente.getInputStream(), StandardCharsets.UTF_8))) {
                    String line = reader.readLine();
                    assertNull(reader.readLine(), "o cliente deve fechar a conexão ao sair");
                    return line;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            try (PublisherClient publisher = new PublisherClient("localhost", brokerFalso.getLocalPort())) {
                publisher.connect();
                publisher.publish("avisos", "manutenção|às 18h");
            }

            var message = Protocol.decode(linhaRecebida.get(3, TimeUnit.SECONDS));
            assertEquals(Command.PUB, message.command());
            assertEquals("avisos", message.topic());
            assertEquals("manutenção|às 18h", message.payload());
        }
    }

    @Test
    void rejeitaPortaForaDoIntervaloValido() {
        assertThrows(IllegalArgumentException.class, () -> new PublisherClient("localhost", 0));
    }
}
