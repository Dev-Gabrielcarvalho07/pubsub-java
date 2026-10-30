package br.edu.fatesg.pubsub.common;

/**
 * Comandos do protocolo de texto trocado entre broker, publishers e subscribers.
 * Cada comando define o tipo da mensagem e, implicitamente, o sentido em que ela trafega.
 */
public enum Command {
    /** Cliente -> broker: assina um tópico. */
    SUB,
    /** Cliente -> broker: cancela a assinatura de um tópico. */
    UNSUB,
    /** Publisher -> broker: publica um conteúdo em um tópico. */
    PUB,
    // [APRESENTAÇÃO] PUB e MSG carregam o mesmo dado, mas são separados para marcar o sentido:
    // o publisher só fala com o broker (PUB) e só o broker entrega aos subscribers (MSG).
    /** Broker -> subscriber: entrega um conteúdo publicado em um tópico assinado. */
    MSG
}
