package br.edu.fatesg.pubsub.broker;

import br.edu.fatesg.pubsub.common.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Testes básicos de inscrição do TopicRegistry.
 * Os testes de entrega de mensagens (PUB) ficam na issue #5.
 */
class TopicRegistryTest {

    /** Cliente falso: em vez de um socket, guarda as mensagens recebidas numa lista. */
    static class FakeSubscriber implements Subscriber {
        final List<Message> recebidas = new ArrayList<>();

        @Override
        public void send(Message message) {
            recebidas.add(message);
        }
    }

    private TopicRegistry registry;
    private FakeSubscriber cliente;

    @BeforeEach
    void setUp() {
        registry = new TopicRegistry();
        cliente = new FakeSubscriber();
    }

    @Test
    void subscribeInscreveClienteNoTopico() {
        registry.subscribe("esportes", cliente);

        assertEquals(1, registry.countSubscribers("esportes"));
    }

    @Test
    void subscribeDuasVezesNaoDuplica() {
        registry.subscribe("esportes", cliente);
        registry.subscribe("esportes", cliente);

        assertEquals(1, registry.countSubscribers("esportes"));
    }

    @Test
    void unsubscribeRemoveClienteDoTopico() {
        registry.subscribe("esportes", cliente);

        registry.unsubscribe("esportes", cliente);

        assertEquals(0, registry.countSubscribers("esportes"));
    }

    @Test
    void unsubscribeDeTopicoDesconhecidoNaoFalha() {
        registry.unsubscribe("inexistente", cliente);

        assertEquals(0, registry.countSubscribers("inexistente"));
    }

    @Test
    void removeFromAllTiraClienteDeTodosOsTopicos() {
        FakeSubscriber outro = new FakeSubscriber();
        registry.subscribe("esportes", cliente);
        registry.subscribe("noticias", cliente);
        registry.subscribe("noticias", outro);

        registry.removeFromAll(cliente);

        assertEquals(0, registry.countSubscribers("esportes"));
        assertEquals(1, registry.countSubscribers("noticias"));
    }

    @Test
    void recusaTopicoVazioOuClienteNulo() {
        assertThrows(IllegalArgumentException.class, () -> registry.subscribe(" ", cliente));
        assertThrows(IllegalArgumentException.class, () -> registry.subscribe("esportes", null));
    }

    @Test
    void publishRecusaConteudoNuloOuVazio() {
        assertThrows(IllegalArgumentException.class, () -> registry.publish("esportes", null));
        assertThrows(IllegalArgumentException.class, () -> registry.publish("esportes", ""));
    }
}
