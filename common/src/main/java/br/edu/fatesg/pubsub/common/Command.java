package br.edu.fatesg.pubsub.common;

/**
 * Comandos do protocolo de texto trocado entre broker, publishers e subscribers.
 */
public enum Command {
    /** Cliente -> broker: assina um tópico. */
    SUB,
    /** Cliente -> broker: cancela a assinatura de um tópico. */
    UNSUB,
    /** Publisher -> broker: publica um conteúdo em um tópico. */
    PUB,
    /** Broker -> subscriber: entrega um conteúdo publicado em um tópico assinado. */
    MSG
}
