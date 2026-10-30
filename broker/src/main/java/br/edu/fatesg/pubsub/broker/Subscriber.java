package br.edu.fatesg.pubsub.broker;

import br.edu.fatesg.pubsub.common.Message;

/**
 * Qualquer cliente que pode receber mensagens do broker.
 *
 * <p>O {@link TopicRegistry} só conhece esta interface, não o socket. Na issue #3
 * o {@code ClientHandler} implementa {@code send} escrevendo a mensagem no socket
 * do cliente; nos testes basta uma implementação que guarda as mensagens numa lista.</p>
 */
public interface Subscriber {

    /** Entrega uma mensagem (normalmente MSG) a este cliente. */
    void send(Message message);
}
