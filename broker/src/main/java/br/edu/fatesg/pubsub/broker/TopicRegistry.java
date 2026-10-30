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
 * Guarda quais clientes estão inscritos em cada tópico e entrega as mensagens publicadas.
 *
 * <p>É o coração do desacoplamento pub/sub: o publisher só diz o tópico, e é aqui
 * que o broker descobre quem deve receber. Publisher e subscriber nunca se conhecem.</p>
 *
 * <p>Vários {@code ClientHandler} (um por thread) chamam esta classe ao mesmo tempo,
 * por isso ela usa coleções seguras para threads:</p>
 * <ul>
 *   <li>{@link ConcurrentHashMap} para o mapa tópico → inscritos;</li>
 *   <li>{@link CopyOnWriteArraySet} para os inscritos de cada tópico: percorrer a lista
 *       durante um PUB é seguro mesmo se alguém fizer SUB/UNSUB no meio.</li>
 * </ul>
 */
public class TopicRegistry {

    private static final Logger LOG = Logger.getLogger(TopicRegistry.class.getName());

    private final Map<String, Set<Subscriber>> topics = new ConcurrentHashMap<>();

    /** SUB: inscreve o cliente no tópico. Inscrever duas vezes não duplica. */
    public void subscribe(String topic, Subscriber subscriber) {
        validar(topic, subscriber);
        // O add fica dentro do compute para ser atômico junto com o remover():
        // assim o cliente nunca é adicionado num conjunto que acabou de sair do mapa.
        topics.compute(topic, (t, inscritos) -> {
            Set<Subscriber> conjunto = (inscritos == null) ? new CopyOnWriteArraySet<>() : inscritos;
            conjunto.add(subscriber);
            return conjunto;
        });
        LOG.info("SUB: cliente inscrito no tópico '" + topic + "'");
    }

    /** UNSUB: remove o cliente do tópico. Se o tópico ficar sem inscritos, ele é apagado. */
    public void unsubscribe(String topic, Subscriber subscriber) {
        validar(topic, subscriber);
        remover(topic, subscriber);
        LOG.info("UNSUB: cliente removido do tópico '" + topic + "'");
    }

    /**
     * PUB: entrega o conteúdo, como mensagem MSG, a todos os inscritos do tópico e só a eles.
     *
     * @return quantos inscritos receberam a mensagem
     */
    public int publish(String topic, String payload) {
        // Valida uma vez aqui; senão o Protocol.encode falharia uma vez para cada inscrito
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException("payload não pode ser nulo nem vazio");
        }
        Message message = new Message(Command.MSG, topic, payload);
        Set<Subscriber> inscritos = topics.getOrDefault(topic, Set.of());

        int entregues = 0;
        for (Subscriber subscriber : inscritos) {
            try {
                subscriber.send(message);
                entregues++;
            } catch (RuntimeException e) {
                // Um cliente com problema não impede a entrega para os outros
                LOG.log(Level.WARNING, "Falha ao entregar mensagem do tópico '" + topic + "'", e);
            }
        }
        LOG.info("Roteamento: tópico '" + topic + "' entregue a " + entregues + " inscrito(s)");
        return entregues;
    }

    /** Remove o cliente de todos os tópicos. Usado quando o cliente desconecta ou cai. */
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

    // computeIfPresent é atômico: ninguém consegue se inscrever no meio da remoção
    // de um tópico vazio e acabar num conjunto que já saiu do mapa.
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
