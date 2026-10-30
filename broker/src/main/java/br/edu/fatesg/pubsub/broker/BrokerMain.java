package br.edu.fatesg.pubsub.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * Ponto de entrada do broker.
 *
 * <p>Porta (em ordem de prioridade):</p>
 * <ol>
 *   <li>primeiro argumento: {@code java ... BrokerMain 6000}</li>
 *   <li>variável de ambiente {@code BROKER_PORT}</li>
 *   <li>padrão {@value #DEFAULT_PORT}</li>
 * </ol>
 */
public class BrokerMain {

    public static final int DEFAULT_PORT = 5000;

    private static final Logger LOG = Logger.getLogger(BrokerMain.class.getName());

    public static void main(String[] args) throws IOException {
        // Formato de log em uma linha: [data hora] NÍVEL mensagem
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] %4$s %5$s%6$s%n");

        int port = resolvePort(args);
        BrokerServer server = new BrokerServer(port, BrokerMain::handlerTemporario);

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

    /**
     * Handler provisório: apenas registra as linhas recebidas e a desconexão.
     * TODO(#3): substituir por {@code new ClientHandler(socket, registry)::run}.
     */
    private static void handlerTemporario(Socket socket) {
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                LOG.info("[" + socket.getRemoteSocketAddress() + "] " + line);
            }
        } catch (IOException e) {
            // cliente caiu sem fechar a conexão; tratado como desconexão
        }
        LOG.info("Cliente desconectado: " + socket.getRemoteSocketAddress());
    }
}
