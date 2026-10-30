package br.edu.fatesg.pubsub.broker;

import br.edu.fatesg.pubsub.common.Command;
import br.edu.fatesg.pubsub.common.Message;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Guarda os inscritos de cada tópico e roteia as publicações: publisher e subscriber nunca se conhecem.
 */
public class TopicRegistry {

    private static final Logger LOG = Logger.getLogger(TopicRegistry.class.getName());

    // [APRESENTAÇÃO] ConcurrentHashMap: vários ClientHandlers usam o mapa ao mesmo tempo; CopyOnWriteArraySet: um PUB percorre os inscritos com segurança mesmo com SUB/UNSUB no meio.
    private final Map<String, Set<Subscriber>> topics = new ConcurrentHashMap<>();

    /** SUB: inscreve o cliente no tópico; inscrever duas vezes não duplica. */
    public void subscribe(String topic, Subscriber subscriber) {
        validar(topic, subscriber);
        // [APRESENTAÇÃO] compute torna "criar o conjunto + adicionar" atômico frente ao remover(), então ninguém entra num conjunto que acabou de sair do mapa.
        topics.compute(topic, (t, inscritos) -> {
            Set<Subscriber> conjunto = (inscritos == null) ? new CopyOnWriteArraySet<>() : inscritos;
            conjunto.add(subscriber);
            return conjunto;
        });
        LOG.info("SUB: cliente " + subscriber + " inscrito no tópico '" + topic + "'");
    }

    /** UNSUB: remove o cliente do tópico e apaga o tópico se ele ficar sem inscritos. */
    public void unsubscribe(String topic, Subscriber subscriber) {
        validar(topic, subscriber);
        remover(topic, subscriber);
        LOG.info("UNSUB: cliente " + subscriber + " removido do tópico '" + topic + "'");
    }

    /** PUB: entrega o conteúdo como MSG a cada inscrito do tópico e retorna quantos receberam. */
    public int publish(String topic, String payload) {
        // Valida uma vez aqui; senão o Protocol.encode falharia uma vez para cada inscrito
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException("payload não pode ser nulo nem vazio");
        }
        Message message = new Message(Command.MSG, topic, payload);
        Set<Subscriber> inscritos = topics.getOrDefault(topic, Set.of());

        // A entrega roda na thread do publisher; a falha de um inscrito não impede a entrega aos outros.
        int entregues = 0;
        for (Subscriber subscriber : inscritos) {
            try {
                subscriber.send(message);
                entregues++;
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Falha ao entregar mensagem do tópico '" + topic + "'", e);
            }
        }
        LOG.info("Roteamento: tópico '" + topic + "' entregue a " + entregues + " inscrito(s)");
        return entregues;
    }

    /** Remove o cliente de todos os tópicos; usado quando ele desconecta ou cai. */
    public void removeFromAll(Subscriber subscriber) {
        if (subscriber == null) {
            throw new IllegalArgumentException("subscriber não pode ser nulo");
        }
        for (String topic : topics.keySet()) {
            remover(topic, subscriber);
        }
        LOG.info("Cliente removido de todos os tópicos");
    }

    /** Quantos clientes estão inscritos no tópico agora. */
    public int countSubscribers(String topic) {
        return topics.getOrDefault(topic, Set.of()).size();
    }

    // computeIfPresent remove o inscrito e apaga o tópico vazio numa única operação atômica.
    private void remover(String topic, Subscriber subscriber) {
        topics.computeIfPresent(topic, (t, inscritos) -> {
            inscritos.remove(subscriber);
            return inscritos.isEmpty() ? null : inscritos;
        });
    }

    private static void validar(String topic, Subscriber subscriber) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic não pode ser nulo nem vazio");
        }
        if (subscriber == null) {
            throw new IllegalArgumentException("subscriber não pode ser nulo");
        }
    }
}
