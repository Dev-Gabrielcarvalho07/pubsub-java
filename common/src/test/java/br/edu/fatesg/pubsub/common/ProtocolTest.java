package br.edu.fatesg.pubsub.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolTest {

    @Nested
    @DisplayName("ida e volta (encode -> decode)")
    class RoundTrip {

        @Test
        void subSemConteudoEhCodificadoEDecodificado() {
            Message original = new Message(Command.SUB, "esportes", null);

            String line = Protocol.encode(original);

            assertEquals("SUB|esportes", line);
            assertEquals(original, Protocol.decode(line));
        }

        @Test
        void unsubSemConteudoEhCodificadoEDecodificado() {
            Message original = new Message(Command.UNSUB, "esportes", null);

            String line = Protocol.encode(original);

            assertEquals("UNSUB|esportes", line);
            assertEquals(original, Protocol.decode(line));
        }

        @Test
        void pubComConteudoEhCodificadoEDecodificado() {
            Message original = new Message(Command.PUB, "esportes", "gol do time A");

            String line = Protocol.encode(original);

            assertEquals("PUB|esportes|gol do time A", line);
            assertEquals(original, Protocol.decode(line));
        }

        @Test
        void msgComConteudoEhCodificadoEDecodificado() {
            Message original = new Message(Command.MSG, "esportes", "gol do time A");

            String line = Protocol.encode(original);

            assertEquals("MSG|esportes|gol do time A", line);
            assertEquals(original, Protocol.decode(line));
        }
    }

    @Nested
    @DisplayName("decode")
    class Decode {

        @Test
        void subEUnsubDecodificamComPayloadNulo() {
            assertNull(Protocol.decode("SUB|esportes").payload());
            assertNull(Protocol.decode("UNSUB|esportes").payload());
        }

        @Test
        void conteudoComSeparadorEhPreservadoNoPub() {
            Message msg = Protocol.decode("PUB|esportes|placar 2|1");

            assertEquals("esportes", msg.topic());
            assertEquals("placar 2|1", msg.payload());
        }

        @Test
        void conteudoComVariosSeparadoresEhPreservadoNoMsg() {
            Message msg = Protocol.decode("MSG|t|a|b|c");

            assertEquals("t", msg.topic());
            assertEquals("a|b|c", msg.payload());
            assertEquals("MSG|t|a|b|c", Protocol.encode(msg));
        }

        @ParameterizedTest(name = "linha [{0}] é rejeitada")
        @NullAndEmptySource
        @ValueSource(strings = {"   "})
        void linhaNulaOuVaziaLancaExcecao(String line) {
            assertThrows(IllegalArgumentException.class, () -> Protocol.decode(line));
        }

        @Test
        void comandoDesconhecidoLancaExcecao() {
            assertThrows(IllegalArgumentException.class, () -> Protocol.decode("FOO|esportes"));
        }

        @ParameterizedTest(name = "comando minúsculo [{0}] é rejeitado")
        @ValueSource(strings = {"sub|esportes", "unsub|esportes", "pub|esportes|x", "msg|esportes|x"})
        void comandoEmMinusculasLancaExcecao(String line) {
            assertThrows(IllegalArgumentException.class, () -> Protocol.decode(line));
        }

        @ParameterizedTest(name = "[{0}] sem conteúdo é rejeitado")
        @ValueSource(strings = {"PUB|esportes", "PUB|esportes|", "MSG|esportes", "MSG|esportes|", "PUB", "MSG"})
        void pubOuMsgSemConteudoLancaExcecao(String line) {
            assertThrows(IllegalArgumentException.class, () -> Protocol.decode(line));
        }

        @ParameterizedTest(name = "[{0}] com conteúdo é rejeitado")
        @ValueSource(strings = {"SUB|esportes|x", "UNSUB|esportes|x"})
        void subOuUnsubComConteudoLancaExcecao(String line) {
            assertThrows(IllegalArgumentException.class, () -> Protocol.decode(line));
        }

        @ParameterizedTest(name = "[{0}] sem tópico é rejeitado")
        @ValueSource(strings = {"SUB", "SUB|", "UNSUB|"})
        void subOuUnsubSemTopicoLancaExcecao(String line) {
            assertThrows(IllegalArgumentException.class, () -> Protocol.decode(line));
        }
    }

    @Nested
    @DisplayName("encode")
    class Encode {

        @Test
        void mensagemNulaLancaExcecao() {
            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(null));
        }

        @Test
        void topicoComSeparadorLancaExcecao() {
            Message msg = new Message(Command.SUB, "a|b", null);

            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(msg));
        }

        @Test
        void pubSemConteudoLancaExcecao() {
            Message msg = new Message(Command.PUB, "esportes", null);

            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(msg));
        }

        @Test
        void pubComConteudoVazioLancaExcecao() {
            Message msg = new Message(Command.PUB, "esportes", "");

            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(msg));
        }

        @Test
        void subComConteudoLancaExcecao() {
            Message msg = new Message(Command.SUB, "esportes", "x");

            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(msg));
        }

        @ParameterizedTest(name = "tópico com quebra de linha [{index}] é rejeitado")
        @ValueSource(strings = {"a\nb", "a\rb", "a\r\nb"})
        void topicoComQuebraDeLinhaLancaExcecao(String topic) {
            Message msg = new Message(Command.SUB, topic, null);

            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(msg));
        }

        @ParameterizedTest(name = "conteúdo com quebra de linha [{index}] é rejeitado")
        @ValueSource(strings = {"x\ny", "x\ry", "x\r\ny"})
        void conteudoComQuebraDeLinhaLancaExcecao(String payload) {
            Message msg = new Message(Command.PUB, "esportes", payload);

            assertThrows(IllegalArgumentException.class, () -> Protocol.encode(msg));
        }
    }
}
