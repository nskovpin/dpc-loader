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
import ru.at_consulting.bigdata.dpc.dim.*;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.cluster.loader.ParserJson;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 26.02.2017.
 */
public class JsonParserTest {

    private Path jsonPath;

    @Before
    public void setup() throws URISyntaxException {
        final String jsonName = "json/trueJson";
        this.jsonPath = Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI());
    }


    @Test
    public void stringifyDate(){
        ProductDim productDim = new ProductDim();

        productDim.setExpirationDate("2999-12-31");
        String date1 = productDim.stringifyExpirationDate();
        Assert.assertTrue(date1.equals("2999-12-31"));

        productDim.setExpirationDate("2017-02-02T13:41:24.0767007+03:00");
        String date2 = productDim.stringifyExpirationDate();
        Assert.assertTrue(date2.equals("2017-02-02"));

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
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);;
        DpcRoot dpcRoot = mapper.readValue(Files.newInputStream(jsonPath), DpcRoot.class);
        Assert.assertNotNull(dpcRoot);
    }

    @Test
    public void jsonStreamParser() throws IOException, URISyntaxException {
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);;
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
    public void realJsonParse() throws URISyntaxException, IOException {
        String jsonName = "json/trueJson";
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        DpcRoot dpcRoot = mapper.readValue(Files.newInputStream(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI())), DpcRoot.class);

        ParserJson parserJson = new ParserJson();
        ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
        Assert.assertTrue(parsedDimsHolder != null);
        Assert.assertTrue(parsedDimsHolder.getRegionDimList().size() > 10);

        String out = "src/test/resources/parsed";
        Files.write(Paths.get(out + File.separator + "product"),parsedDimsHolder.getProductDim().stringify().getBytes());

        String allRegions = "";
        for(RegionDim s : parsedDimsHolder.getRegionDimList() ){
            allRegions += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "region"), allRegions.getBytes());

        String allExternal = "";
        for(ExternalRegionMappingDim s : parsedDimsHolder.getExternalRegionMappingDimList()){
            allExternal += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "external"), allExternal.getBytes());

        Files.write(Paths.get(out + File.separator + "market"), parsedDimsHolder.getMarketingProductDim().stringify().getBytes());

        String allLinks = "";
        for(ProductRegionLinkDim s : parsedDimsHolder.getProductRegionLinkDimList()){
            allLinks += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "links"), allLinks.getBytes());

        String allWebs = "";
        for(WebEntityDim s : parsedDimsHolder.getWebEntityDimList()){
            allWebs += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "web"), allWebs.getBytes());

        String productMaps = "";
        for(ProductMapDim s : parsedDimsHolder.getProductMapDimList()){
            productMaps += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "productMap"), productMaps.getBytes());
    }


    @Test
    public void realJsonParse2() throws URISyntaxException, IOException {
        String jsonName = "json/trueJson2";
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        DpcRoot dpcRoot = mapper.readValue(Files.newInputStream(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI())), DpcRoot.class);

        ParserJson parserJson = new ParserJson();
        ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
        Assert.assertTrue(parsedDimsHolder != null);
        Assert.assertTrue(parsedDimsHolder.getRegionDimList().size() > 10);
    }


    @Test
    public void readAllJsonParse() throws URISyntaxException, IOException {
        String jsonName = "json/dataTest.csv";
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        List<String> lines = Files.readAllLines(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI()), Charset.forName("UTF-8"));

        List<ParserJson.ParsedDimsHolder> list = new ArrayList<>();
        for(String line: lines){
            DpcRoot dpcRoot = mapper.readValue(line, DpcRoot.class);
            ParserJson parserJson = new ParserJson();
            ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
            list.add(parsedDimsHolder);
            Assert.assertTrue(parsedDimsHolder != null);
        }
        Assert.assertTrue(list.size() > 500);

        List<WebEntityDim> webEntityDimList = new ArrayList<>();
        for(ParserJson.ParsedDimsHolder parsedDimsHolder: list){
            if(parsedDimsHolder.getWebEntityDimList()!= null){
                webEntityDimList.addAll(parsedDimsHolder.getWebEntityDimList());
            }
        }
        Assert.assertNotNull(webEntityDimList);
        String out = "src/test/resources/parsed";

        String allWebs = "";
        for(WebEntityDim s : webEntityDimList){
            allWebs += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "webList"), allWebs.getBytes());
    }

    @Test
    public void readAllJsonParse3() throws URISyntaxException, IOException {
        String jsonName = "json/14dpc.csv";
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        List<String> lines = Files.readAllLines(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI()), Charset.forName("UTF-8"));

        List<ParserJson.ParsedDimsHolder> list = new ArrayList<>();
        for(String line: lines){
            DpcRoot dpcRoot = mapper.readValue(line, DpcRoot.class);
            ParserJson parserJson = new ParserJson();
            ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
            list.add(parsedDimsHolder);
            Assert.assertTrue(parsedDimsHolder != null);
        }
        Assert.assertTrue(list.size() > 0);

        List<WebEntityDim> webEntityDimList = new ArrayList<>();
        for(ParserJson.ParsedDimsHolder parsedDimsHolder: list){
            if(parsedDimsHolder.getWebEntityDimList()!= null){
                webEntityDimList.addAll(parsedDimsHolder.getWebEntityDimList());
            }
        }
        Assert.assertNotNull(webEntityDimList);
        String out = "src/test/resources/parsed";

        String allWebs = "";
        for(WebEntityDim s : webEntityDimList){
            allWebs += s.stringify() + "\n";
        }
        Files.write(Paths.get(out + File.separator + "webList3"), allWebs.getBytes());
    }

    @Test
    public void readAllJsonParse2() throws URISyntaxException, IOException {
        String jsonName = "json/dpc.csv";
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        List<String> lines = Files.readAllLines(Paths.get(JsonParserTest.class.getResource("/" + jsonName).toURI()), Charset.forName("UTF-8"));

        List<ParserJson.ParsedDimsHolder> list = new ArrayList<>();
        for(String line: lines){
            DpcRoot dpcRoot = mapper.readValue(line, DpcRoot.class);
            ParserJson parserJson = new ParserJson();
            ParserJson.ParsedDimsHolder parsedDimsHolder = parserJson.parseDcpRoot(dpcRoot);
            list.add(parsedDimsHolder);
            Assert.assertTrue(parsedDimsHolder != null);
        }
        Assert.assertTrue(list.size() == 11);
    }

}
