package br.edu.fatesg.pubsub.common;

import java.util.regex.Pattern;

/**
 * Codifica e decodifica mensagens do protocolo de texto (uma mensagem por linha).
 * É o único contrato compartilhado por broker, publishers e subscribers: publisher e subscriber não se
 * conhecem, e a classe, sem estado, pode ser usada ao mesmo tempo por várias threads.
 *
 * <pre>
 * SUB|topico
 * UNSUB|topico
 * PUB|topico|conteudo
 * MSG|topico|conteudo
 * </pre>
 */
public final class Protocol {

    /** Separador entre os campos de uma linha do protocolo. */
    public static final String SEPARATOR = "|";

    private static final Pattern SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(SEPARATOR));

    private Protocol() {
    }

    /**
     * Transforma a mensagem na linha de texto do protocolo (sem quebra de linha no final).
     * Usado por quem envia: publisher (PUB), subscriber (SUB/UNSUB) e broker (MSG).
     */
    public static String encode(Message msg) {
        if (msg == null) {
            throw new IllegalArgumentException("mensagem não pode ser nula");
        }
        // O tópico é sempre o segundo campo; se contivesse o separador, o decode o cortaria errado.
        if (msg.topic().contains(SEPARATOR)) {
            throw new IllegalArgumentException("topic não pode conter o separador '" + SEPARATOR + "': " + msg.topic());
        }
        if (containsLineBreak(msg.topic())) {
            throw new IllegalArgumentException("topic não pode conter quebra de linha (\\n ou \\r)");
        }

        String prefix = msg.command() + SEPARATOR + msg.topic();
        if (hasPayload(msg.command())) {
            if (msg.payload() == null || msg.payload().isEmpty()) {
                throw new IllegalArgumentException(msg.command() + " exige conteúdo");
            }
            if (containsLineBreak(msg.payload())) {
                throw new IllegalArgumentException("conteúdo não pode conter quebra de linha (\\n ou \\r)");
            }
            return prefix + SEPARATOR + msg.payload();
        }
        if (msg.payload() != null) {
            throw new IllegalArgumentException(msg.command() + " não aceita conteúdo");
        }
        return prefix;
    }

    /**
     * Interpreta uma linha do protocolo. O conteúdo de PUB e MSG pode conter o separador.
     * Usado por quem recebe: o broker (SUB/UNSUB/PUB) e o subscriber (MSG).
     */
    public static Message decode(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("linha não pode ser nula nem vazia");
        }

        // [APRESENTAÇÃO] Limite de 3 partes: comando, tópico e o resto da linha como conteúdo,
        // assim o conteúdo pode conter "|" sem ser quebrado. Por isso o tópico não pode ter "|".
        String[] parts = SEPARATOR_PATTERN.split(line, 3);
        Command command = parseCommand(parts[0], line);

        if (hasPayload(command)) {
            if (parts.length < 3 || parts[2].isEmpty()) {
                throw new IllegalArgumentException(
                        command + " exige o formato " + command + SEPARATOR + "topico" + SEPARATOR + "conteudo: " + line);
            }
            return new Message(command, parts[1], parts[2]);
        }

        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    command + " exige o formato " + command + SEPARATOR + "topico: " + line);
        }
        return new Message(command, parts[1], null);
    }

    private static Command parseCommand(String token, String line) {
        try {
            return Command.valueOf(token);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("comando desconhecido '" + token + "' na linha: " + line, e);
        }
    }

    // [APRESENTAÇÃO] TCP é um fluxo de bytes sem fronteiras de mensagem; o enquadramento é feito
    // pela quebra de linha (readLine), então nenhum campo pode conter \n ou \r.
    private static boolean containsLineBreak(String value) {
        return value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0;
    }

    private static boolean hasPayload(Command command) {
        return command == Command.PUB || command == Command.MSG;
    }
}
