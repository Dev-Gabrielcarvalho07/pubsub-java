package br.edu.fatesg.pubsub.publisher;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/** Interface de terminal para escolher tópico e publicar mensagens. */
public class PublisherMain {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5000;

    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] %4$s %5$s%6$s%n");

        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port;
        try {
            port = args.length > 1 ? Integer.parseInt(args[1].trim()) : DEFAULT_PORT;
            if (port < 1 || port > 65_535) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.err.println("Porta inválida. Use um número entre 1 e 65535.");
            System.exit(2);
            return;
        }

        try (PublisherClient client = new PublisherClient(host, port)) {
            client.connect();
            System.out.println("Conectado ao broker " + host + ":" + port + ".");
            System.out.println("Digite 'sair' no campo de tópico para encerrar.");
            runPrompt(client);
        } catch (IOException e) {
            System.err.println("Não foi possível comunicar com o broker " + host + ":" + port + ": " + e.getMessage());
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.err.println("Configuração inválida: " + e.getMessage());
            System.exit(2);
        }
    }

    private static void runPrompt(PublisherClient client) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, terminalCharset()));
        while (true) {
            System.out.print("Tópico (ou 'sair' para encerrar): ");
            String topic = in.readLine();
            if (topic == null || topic.trim().equalsIgnoreCase("sair")) {
                return;
            }
            topic = topic.trim();
            if (topic.isEmpty()) {
                System.out.println("Informe um tópico.");
                continue;
            }

            System.out.print("Mensagem: ");
            String payload = in.readLine();
            if (payload == null) {
                return;
            }

            try {
                client.publish(topic, payload);
                System.out.println("Mensagem publicada no tópico '" + topic + "'.");
            } catch (IllegalArgumentException e) {
                System.out.println("Mensagem inválida: " + e.getMessage());
            }
        }
    }

    private static Charset terminalCharset() {
        Console console = System.console();
        return console != null ? console.charset() : Charset.defaultCharset();
    }
}
