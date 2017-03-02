package ru.at_consulting.bigdata.dpc.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.loader.ParserJson;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by NSkovpin on 26.02.2017.
 */
public class JsonParserTest {

    private Path jsonPath;

    @Before
    public void setup() throws URISyntaxException {
        final String jsonName = "json/testParse";
        this.jsonPath = Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI());
    }

    @Test
    public void jsonDomParser() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DpcRoot dpcRoot = mapper.readValue(Files.newInputStream(jsonPath), DpcRoot.class);
        Assert.assertNotNull(dpcRoot);
    }

    @Test
    public void jsonStreamParser() throws IOException, URISyntaxException {
        ObjectMapper mapper = new ObjectMapper();
        Path arrayPath = Paths.get(JsonParserTest.class.getResource("/json/arrayParse").toURI());
        JsonParser parser = mapper.getFactory().createParser(Files.newInputStream(arrayPath));
        if(parser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected an array");
        }
        while(parser.nextToken() == JsonToken.START_OBJECT) {
            ObjectNode node = mapper.readTree(parser);
            DpcRoot newJsonNode = mapper.treeToValue(node, DpcRoot.class);
            Assert.assertNotNull(newJsonNode);
        }

        parser.close();
    }

    @Test
    public void jsonParse() throws IOException, URISyntaxException {
        String jsonName = "json/dimHolder";
        ObjectMapper mapper = new ObjectMapper();
        DpcRoot dpcRoot = mapper.readValue(Files.newInputStream(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI())), DpcRoot.class);

        ParserJson parserJson = new ParserJson();
        ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
        Assert.assertTrue(parsedDimsHolder != null);
    }

}
