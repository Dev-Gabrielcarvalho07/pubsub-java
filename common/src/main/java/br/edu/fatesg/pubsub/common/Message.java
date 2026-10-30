package br.edu.fatesg.pubsub.common;

/**
 * Mensagem do protocolo pub/sub.
 *
 * @param command comando da mensagem (obrigatório)
 * @param topic   tópico alvo (obrigatório, não vazio)
 * @param payload conteúdo da mensagem; pode ser {@code null} em SUB e UNSUB
 */
public record Message(Command command, String topic, String payload) {

    public Message {
        if (command == null) {
            throw new IllegalArgumentException("command não pode ser nulo");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic não pode ser nulo nem vazio");
        }
    }
}
