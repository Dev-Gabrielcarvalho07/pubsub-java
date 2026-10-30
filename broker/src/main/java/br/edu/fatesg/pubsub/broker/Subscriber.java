package br.edu.fatesg.pubsub.broker;

import br.edu.fatesg.pubsub.common.Message;

/**
 * Destinatário de mensagens do broker; o {@link TopicRegistry} só conhece esta interface, não o socket.
 */
public interface Subscriber {

    /** Entrega uma mensagem (normalmente MSG) a este cliente. */
    void send(Message message);
}
