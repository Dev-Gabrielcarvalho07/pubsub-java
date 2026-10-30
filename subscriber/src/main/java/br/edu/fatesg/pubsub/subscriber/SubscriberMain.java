package br.edu.fatesg.pubsub.subscriber;

import br.edu.fatesg.pubsub.common.Message;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * Ponto de entrada do subscriber: conecta ao broker e lê comandos do terminal.
 *
 * <p>Uso: {@code java ... SubscriberMain [host] [porta]} (padrão {@value #DEFAULT_HOST} {@value #DEFAULT_PORT}).</p>
 *
 * <pre>
 * sub &lt;topico&gt;    assina um tópico
 * unsub &lt;topico&gt;  cancela a assinatura
 * sair             encerra o programa
 * </pre>
 */
public class SubscriberMain {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5000;

    private static final long CONNECTION_CHECK_INTERVAL_MS = 500;

    private static final String HELP = """
            Comandos:
              sub <topico>    assina um tópico
              unsub <topico>  cancela a assinatura de um tópico
              sair            encerra o programa""";

    private static volatile boolean quitting;

    public static void main(String[] args) {
        // Formato de log em uma linha: [data hora] NÍVEL mensagem
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] %4$s %5$s%6$s%n");

        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port;
        try {
            port = args.length > 1 ? Integer.parseInt(args[1].trim()) : DEFAULT_PORT;
        } catch (NumberFormatException e) {
            System.err.println("Porta inválida: " + args[1]);
            System.exit(2);
            return;
        }

        SubscriberClient client = new SubscriberClient(host, port, SubscriberMain::printMessage);
        try {
            client.connect();
        } catch (IOException e) {
            System.err.println("Não foi possível conectar ao broker " + host + ":" + port + ": " + e.getMessage());
            System.exit(1);
            return;
        }

        // Ctrl+C encerra a conexão de forma limpa
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        startConnectionWatcher(client);

        System.out.println("Conectado ao broker " + host + ":" + port + ".");
        System.out.println(HELP);

        try {
            runCommandLoop(client);
        } catch (IOException e) {
            System.err.println("Erro ao ler o terminal: " + e.getMessage());
        }

        quitting = true;
        client.close();
    }

    private static void runCommandLoop(SubscriberClient client) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, terminalCharset()));
        String line;
        while ((line = in.readLine()) != null) {
            String[] parts = line.trim().split("\\s+", 2);
            String command = parts[0].toLowerCase();
            String topic = parts.length > 1 ? parts[1].trim() : "";

            switch (command) {
                case "" -> {
                    // linha em branco: ignora
                }
                case "sair" -> {
                    return;
                }
                case "sub", "unsub" -> {
                    if (topic.isEmpty()) {
                        System.out.println("Informe o tópico: " + command + " <topico>");
                        continue;
                    }
                    send(client, command, topic);
                }
                default -> {
                    System.out.println("Comando inválido: " + line.trim());
                    System.out.println(HELP);
                }
            }
        }
    }

    private static void send(SubscriberClient client, String command, String topic) {
        try {
            if (command.equals("sub")) {
                client.subscribe(topic);
                System.out.println("Assinado: " + topic);
            } else {
                client.unsubscribe(topic);
                System.out.println("Assinatura cancelada: " + topic);
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Tópico inválido: " + e.getMessage());
        } catch (IllegalStateException | IOException e) {
            exitConnectionLost();
        }
    }

    /**
     * A leitura do terminal fica bloqueada esperando o usuário, então uma thread separada verifica a conexão e
     * encerra o programa se o broker cair.
     */
    private static void startConnectionWatcher(SubscriberClient client) {
        Thread watcher = new Thread(() -> {
            while (!quitting) {
                if (!client.isConnected()) {
                    exitConnectionLost();
                    return;
                }
                try {
                    Thread.sleep(CONNECTION_CHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "subscriber-connection-watcher");
        watcher.setDaemon(true);
        watcher.start();
    }

    private static void exitConnectionLost() {
        if (quitting) {
            return;
        }
        System.err.println("Conexão com o broker perdida. Encerrando.");
        System.exit(1);
    }

    private static void printMessage(Message msg) {
        System.out.println("[" + msg.topic() + "] " + msg.payload());
    }

    private static Charset terminalCharset() {
        Console console = System.console();
        return console != null ? console.charset() : Charset.defaultCharset();
    }
}
