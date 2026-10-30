package br.edu.fatesg.pubsub.broker;

import br.edu.fatesg.pubsub.common.Command;
import br.edu.fatesg.pubsub.common.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testes do TopicRegistry: inscrição e entrega de mensagens (PUB).
 */
class TopicRegistryTest {

    /**
     * Cliente falso: em vez de um socket, guarda as mensagens recebidas numa lista.
     * A lista é thread-safe para o teste de concorrência.
     */
    static class FakeSubscriber implements Subscriber {
        final List<Message> recebidas = new CopyOnWriteArrayList<>();

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

    // ---- entrega de mensagens (issue #5) ----

    @Test
    void inscritoRecebeMsgComTopicoEConteudoPublicados() {
        registry.subscribe("esportes", cliente);

        registry.publish("esportes", "gol do time A");

        assertEquals(List.of(new Message(Command.MSG, "esportes", "gol do time A")), cliente.recebidas);
    }

    @Test
    void conteudoComSeparadorChegaIntactoAoInscrito() {
        registry.subscribe("esportes", cliente);

        registry.publish("esportes", "placar 2|1");

        assertEquals("placar 2|1", cliente.recebidas.get(0).payload());
    }

    @Test
    void clienteNaoRecebeMensagemDeTopicoQueNaoAssinou() {
        FakeSubscriber inscritoEmNoticias = new FakeSubscriber();
        registry.subscribe("esportes", cliente);
        registry.subscribe("noticias", inscritoEmNoticias);

        registry.publish("esportes", "gol do time A");

        assertTrue(inscritoEmNoticias.recebidas.isEmpty());
    }

    @Test
    void depoisDoUnsubscribeClienteParaDeReceber() {
        registry.subscribe("esportes", cliente);
        registry.publish("esportes", "antes");

        registry.unsubscribe("esportes", cliente);
        int entregues = registry.publish("esportes", "depois");

        assertEquals(0, entregues);
        assertEquals(List.of("antes"), payloads(cliente));
    }

    @Test
    void depoisDoRemoveFromAllClienteNaoRecebeDeNenhumTopico() {
        registry.subscribe("esportes", cliente);
        registry.subscribe("noticias", cliente);

        registry.removeFromAll(cliente);
        registry.publish("esportes", "gol do time A");
        registry.publish("noticias", "chuva amanhã");

        assertTrue(cliente.recebidas.isEmpty());
    }

    @Test
    void doisInscritosNoMesmoTopicoRecebemEPublishRetornaDois() {
        FakeSubscriber outro = new FakeSubscriber();
        registry.subscribe("esportes", cliente);
        registry.subscribe("esportes", outro);

        int entregues = registry.publish("esportes", "gol do time A");

        assertEquals(2, entregues);
        assertEquals(List.of("gol do time A"), payloads(cliente));
        assertEquals(List.of("gol do time A"), payloads(outro));
    }

    @Test
    void clienteInscritoDuasVezesRecebeAMensagemUmaVezSo() {
        registry.subscribe("esportes", cliente);
        registry.subscribe("esportes", cliente);

        int entregues = registry.publish("esportes", "gol do time A");

        assertEquals(1, entregues);
        assertEquals(1, cliente.recebidas.size());
    }

    @Test
    void publicarEmTopicoSemInscritosRetornaZeroSemErro() {
        int entregues = registry.publish("ninguem-assinou", "alô?");

        assertEquals(0, entregues);
    }

    @Test
    void publishComConteudoInvalidoNaoEntregaNadaAosInscritos() {
        registry.subscribe("esportes", cliente);

        assertThrows(IllegalArgumentException.class, () -> registry.publish("esportes", null));
        assertThrows(IllegalArgumentException.class, () -> registry.publish("esportes", ""));

        assertTrue(cliente.recebidas.isEmpty());
    }

    @Test
    void inscritoComFalhaNoSendNaoImpedeEntregaAosOutros() {
        // inscrito primeiro, para a falha acontecer antes da entrega ao cliente
        Subscriber comFalha = message -> {
            throw new IllegalStateException("socket fechado");
        };
        registry.subscribe("esportes", comFalha);
        registry.subscribe("esportes", cliente);

        int entregues = registry.publish("esportes", "gol do time A");

        assertEquals(1, entregues);
        assertEquals(List.of("gol do time A"), payloads(cliente));
    }

    @Test
    void inscricoesEPublicacoesConcorrentesNaoPerdemInscricoesNemLancamExcecao() throws Exception {
        int inscritos = 50;
        int publicadores = 10;
        int publicacoesPorPublicador = 20;
        List<FakeSubscriber> clientes = new ArrayList<>();
        for (int i = 0; i < inscritos; i++) {
            clientes.add(new FakeSubscriber());
        }

        ExecutorService pool = Executors.newFixedThreadPool(inscritos + publicadores);
        CountDownLatch largada = new CountDownLatch(1);
        List<Future<?>> tarefas = new ArrayList<>();
        try {
            for (FakeSubscriber c : clientes) {
                tarefas.add(pool.submit(() -> {
                    largada.await();
                    registry.subscribe("esportes", c);
                    return null;
                }));
            }
            for (int p = 0; p < publicadores; p++) {
                tarefas.add(pool.submit(() -> {
                    largada.await();
                    for (int i = 0; i < publicacoesPorPublicador; i++) {
                        registry.publish("esportes", "concorrente");
                    }
                    return null;
                }));
            }

            // libera todas as threads ao mesmo tempo
            largada.countDown();

            // get() relança aqui qualquer exceção que tenha acontecido dentro das threads
            for (Future<?> tarefa : tarefas) {
                tarefa.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(inscritos, registry.countSubscribers("esportes"));

        // depois da corrida, uma publicação final precisa chegar a todos os inscritos
        int entregues = registry.publish("esportes", "final");

        assertEquals(inscritos, entregues);
        for (FakeSubscriber c : clientes) {
            assertEquals("final", c.recebidas.get(c.recebidas.size() - 1).payload());
        }
    }

    private static List<String> payloads(FakeSubscriber subscriber) {
        return subscriber.recebidas.stream().map(Message::payload).toList();
    }
}
