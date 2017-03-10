package ru.at_consulting.bigdata.dpc.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.at_consulting.bigdata.dpc.dim.RegionDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.*;

/**
 * Created by NSkovpin on 26.02.2017.
 */
public class JsonParserTest {

    private Path jsonPath;

    @Before
    public void setup() throws URISyntaxException {
        final String jsonName = "json/dimHolder";
        this.jsonPath = Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI());
    }

    @Test
    public void parseJsonDate(){
        DateTime dateTime = DateTime.parse("2017-02-27T20:47:14.7119741+03:00");
        System.out.println(dateTime);
        DateTime dateTime1 = DateTime.parse("2017-02-27T20:46:56.086815+03:00");

        Assert.assertTrue(dateTime1.isBefore(dateTime));
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
        Assert.assertTrue(parsedDimsHolder.getMarketingProductDim() != null);

        Path market = Paths.get(JsonParserTest.class.getResource("/parsed/" + "market").toURI());
        String str = parsedDimsHolder.getMarketingProductDim().stringify();
        Assert.assertTrue(str != null);
        Files.write(market, str.getBytes());

        Path product = Paths.get(JsonParserTest.class.getResource("/parsed/" + "product").toURI());
        String strProduct = parsedDimsHolder.getProductDim().stringify();
        Files.write(product, strProduct.getBytes());

        Path region = Paths.get(JsonParserTest.class.getResource("/parsed/" + "region").toURI());
        String allRegions = "";
        for(RegionDim s : parsedDimsHolder.getRegionDimList() ){
            allRegions += s.stringify() + "\n";
        }
        Files.write(region, allRegions.getBytes());
    }

    @Test
    public void realJsonParse() throws URISyntaxException, IOException {
        String jsonName = "json/trueJson";
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);;
        DpcRoot dpcRoot = mapper.readValue(Files.newInputStream(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI())), DpcRoot.class);

        ParserJson parserJson = new ParserJson();
        ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
        Assert.assertTrue(parsedDimsHolder != null);
        Assert.assertTrue(parsedDimsHolder.getRegionDimList().size() > 10);
    }

}
