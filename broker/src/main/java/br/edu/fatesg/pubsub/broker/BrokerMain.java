package br.edu.fatesg.pubsub.broker;

import java.io.IOException;

/**
 * Ponto de entrada do broker; a porta vem do 1º argumento, de BROKER_PORT ou do padrão {@value #DEFAULT_PORT}.
 */
public class BrokerMain {

    public static final int DEFAULT_PORT = 5000;

    /** Cria o registry compartilhado e inicia o servidor, que roda até o processo ser encerrado. */
    public static void main(String[] args) throws IOException {
        // Formato de log em uma linha: [data hora] NÍVEL mensagem
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] %4$s %5$s%6$s%n");

        int port = resolvePort(args);
        // Um único registry compartilhado por todos os clientes: é ele que liga publishers a subscribers.
        TopicRegistry registry = new TopicRegistry();
        BrokerServer server = new BrokerServer(port, socket -> new ClientHandler(socket, registry).run());

        // Ctrl+C encerra o broker de forma limpa
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

        server.start();
    }

    static int resolvePort(String[] args) {
        String value = args.length > 0 ? args[0] : System.getenv("BROKER_PORT");
        if (value == null || value.isBlank()) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Porta inválida: " + value, e);
        }
    }
}
