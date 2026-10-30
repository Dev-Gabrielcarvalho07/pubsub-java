package br.edu.fatesg.pubsub.common;

/**
 * Mensagem do protocolo pub/sub: unidade trocada entre clientes e broker.
 * As regras de formato por comando (ex.: PUB exige conteúdo) são aplicadas em {@link Protocol}.
 *
 * @param command comando da mensagem (obrigatório)
 * @param topic   tópico alvo (obrigatório, não vazio)
 * @param payload conteúdo da mensagem; pode ser {@code null} em SUB e UNSUB
 */
// [APRESENTAÇÃO] Por ser um record imutável, pode ser compartilhado entre threads sem sincronização.
public record Message(Command command, String topic, String payload) {

    /**
     * Valida os campos obrigatórios na criação, rejeitando mensagens sem comando ou sem tópico.
     */
    public Message {
        if (command == null) {
            throw new IllegalArgumentException("command não pode ser nulo");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic não pode ser nulo nem vazio");
        }
    }
}
