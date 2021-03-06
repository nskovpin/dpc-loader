package ru.at_consulting.bigdata.dpc.cluster.loader;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.at_consulting.bigdata.dpc.dim.*;
import ru.at_consulting.bigdata.dpc.dim.creator.*;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.marketing.MarketingProduct;
import ru.at_consulting.bigdata.dpc.json.products.Product;
import ru.at_consulting.bigdata.dpc.json.region.ExternalRegionMapping;
import ru.at_consulting.bigdata.dpc.json.region.Region;
import ru.at_consulting.bigdata.dpc.json.region.Regions;
import ru.at_consulting.bigdata.dpc.json.webentity.WebEntity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class ParserJson {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParserJson.class);

    public ParserJson() {

    }

    public void parseSingleObject(InputStream inputStream, Configuration configuration, String outputFolder) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DpcRoot dpcRoot = mapper.readValue(inputStream, DpcRoot.class);
        ParsedDimsHolder parsedDimsHolder = parseDcpRoot(dpcRoot);
        saveObject(parsedDimsHolder, configuration, outputFolder, false);
    }

    public void parseArrayObject(InputStream inputStream, Configuration configuration, String outputFolder) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser parser = mapper.getFactory().createParser(inputStream);
        if (parser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected an array");
        }
        while (parser.nextToken() == JsonToken.START_OBJECT) {
            ObjectNode node = mapper.readTree(parser);
            DpcRoot dpcRoot = mapper.treeToValue(node, DpcRoot.class);
            ParsedDimsHolder parsedDimsHolder = parseDcpRoot(dpcRoot);
            saveObject(parsedDimsHolder, configuration, outputFolder, true);
        }
        parser.close();
    }

    public ParsedDimsHolder parseDcpRoot(DpcRoot dpcRoot) {
        ParsedDimsHolder parsedDimsHolder = new ParsedDimsHolder();
        try {
            if (dpcRoot.getProductInfo() != null && dpcRoot.getProductInfo().getProducts() != null &&
                    dpcRoot.getProductInfo().getProducts().getProduct() != null && dpcRoot.getAction() != null) {
                DimCreator<ProductDim, Product> productCreator = DimCreatorFactory.getCreator(ProductDim.class);
                ProductDim productDim = productCreator.create(dpcRoot,
                        dpcRoot.getProductInfo().getProducts().getProduct());
                parsedDimsHolder.setProductDim(productDim);

                if (dpcRoot.getAction().equals(DpcRoot.PUT)) {
                    parsedDimsHolder.setDelete(false);
                } else if (dpcRoot.getAction().equals(DpcRoot.DELETE)) {
                    parsedDimsHolder.setDelete(true);
                    return parsedDimsHolder;
                }

                if (dpcRoot.getProductInfo().getProducts().getProduct().getMarketingProduct() != null) {

                    DimCreator<MarketingProductDim, MarketingProduct> marketingCreator = DimCreatorFactory.getCreator(MarketingProductDim.class);
                    MarketingProductDim marketingProductDim = marketingCreator.create(dpcRoot,
                            dpcRoot.getProductInfo().getProducts().getProduct().getMarketingProduct());
                    parsedDimsHolder.setMarketingProductDim(marketingProductDim);

                }

                List<ProductMapDim> productMapDimList = new ArrayList<>();
                ProductMapDimCreator productMapDimCreator = DimCreatorFactory.getProductMapDimCreator();

                List<WebEntityDim> webEntityDimList = null;
                if (dpcRoot.getProductInfo().getProducts().getProduct().getProductWebEntities() != null) {

                    DimCreator<WebEntityDim, WebEntity> webEntityCreator = DimCreatorFactory.getCreator(WebEntityDim.class);
                    webEntityDimList = webEntityCreator.create(dpcRoot,
                            dpcRoot.getProductInfo().getProducts().getProduct().getProductWebEntities());
                    parsedDimsHolder.setWebEntityDimList(webEntityDimList);
                }

                Regions regions = dpcRoot.getProductInfo().getProducts().getProduct().getRegions();
                if (regions != null && regions.getRegion() != null) {

                    List<ProductRegionLinkDim> productRegionLinkDimList = null;
                    DimCreator<ProductRegionLinkDim, Region> productRegionLinkCreator = DimCreatorFactory.getCreator(ProductRegionLinkDim.class);

                    List<RegionDim> regionDimList = null;
                    DimCreator<RegionDim, Region> regionDimCreator = DimCreatorFactory.getCreator(RegionDim.class);

                    List<ExternalRegionMappingDim> externalRegionMappingDimList = null;
                    DimCreator<ExternalRegionMappingDim, ExternalRegionMapping> externalRegionMappingDimCreator
                            = DimCreatorFactory.getCreator(ExternalRegionMappingDim.class);

                    for (Region region : regions.getRegion()) {
                        if (productRegionLinkDimList == null) {
                            productRegionLinkDimList = new ArrayList<>();
                        }
                        productRegionLinkDimList.add(productRegionLinkCreator.create(dpcRoot, region));

                        if (regionDimList == null) {
                            regionDimList = new ArrayList<>();
                        }
                        regionDimList.add(regionDimCreator.create(dpcRoot, region));

                        List<ExternalRegionMappingDim> subExternalMapping = null;
                        if (region.getExternalRegionMappings() != null && region.getExternalRegionMappings().getExternalRegionMapping() != null) {

                            if (externalRegionMappingDimList == null) {
                                externalRegionMappingDimList = new ArrayList<>();
                            }
                            subExternalMapping = externalRegionMappingDimCreator.create(dpcRoot,
                                    region.getExternalRegionMappings());

                            for (ExternalRegionMappingDim sub : subExternalMapping) {
                                sub.setRegionId(region.getId());
                            }

                            externalRegionMappingDimList.addAll(subExternalMapping);
                        }

                        productMapDimList.addAll(productMapDimCreator.create(dpcRoot, region, webEntityDimList, subExternalMapping));
                    }

                    parsedDimsHolder.setProductRegionLinkDimList(productRegionLinkDimList);
                    parsedDimsHolder.setRegionDimList(regionDimList);
                    parsedDimsHolder.setExternalRegionMappingDimList(externalRegionMappingDimList);
                } else {
                    productMapDimList.addAll(productMapDimCreator.create(dpcRoot, null, webEntityDimList, null));
                }

                parsedDimsHolder.setProductMapDimList(productMapDimList);
            }
        } catch (Exception e) {
            LOGGER.error("Json parse exception", e);
        }
        return parsedDimsHolder;
    }


    private void saveObject(ParsedDimsHolder parsedDimsHolder, Configuration configuration, String outputFolder, boolean append) throws IOException {
        BufferedWriter bufferedWriter = getDimWriter(configuration, outputFolder, append, parsedDimsHolder.getProductDim());
        HdfsWriter.writeLine(bufferedWriter, parsedDimsHolder.getProductDim().stringify());
        HdfsWriter.closeWriter(bufferedWriter);
        if (!parsedDimsHolder.isDelete()) {
            List<RegionDim> regionDimList = parsedDimsHolder.getRegionDimList();
            BufferedWriter regionWriter = getDimWriter(configuration, outputFolder, append, RegionDim.class);
            for (RegionDim regionDim : regionDimList) {
                HdfsWriter.writeLine(regionWriter, regionDim.stringify());
            }
            HdfsWriter.closeWriter(regionWriter);

            List<ExternalRegionMappingDim> externalRegionMappingDimList = parsedDimsHolder.getExternalRegionMappingDimList();
            BufferedWriter externalRegionMappingWriter = getDimWriter(configuration, outputFolder, append, ExternalRegionMappingDim.class);
            for (ExternalRegionMappingDim externalRegionMappingDim : externalRegionMappingDimList) {
                HdfsWriter.writeLine(externalRegionMappingWriter, externalRegionMappingDim.stringify());
            }
            HdfsWriter.closeWriter(externalRegionMappingWriter);

            List<ProductRegionLinkDim> productRegionLinkDimList = parsedDimsHolder.getProductRegionLinkDimList();
            BufferedWriter productRegionLinkWriter = getDimWriter(configuration, outputFolder, append, ProductRegionLinkDim.class);
            for (ProductRegionLinkDim productRegionLinkDim : productRegionLinkDimList) {
                HdfsWriter.writeLine(productRegionLinkWriter, productRegionLinkDim.stringify());
            }
            HdfsWriter.closeWriter(productRegionLinkWriter);

            MarketingProductDim marketingProductDim = parsedDimsHolder.getMarketingProductDim();
            BufferedWriter marketingProductWriter = getDimWriter(configuration, outputFolder, append, marketingProductDim);
            HdfsWriter.writeLine(marketingProductWriter, marketingProductDim.stringify());
            HdfsWriter.closeWriter(marketingProductWriter);

            List<WebEntityDim> webEntityDimList = parsedDimsHolder.getWebEntityDimList();
            BufferedWriter webEntityDimWriter = getDimWriter(configuration, outputFolder, append, WebEntityDim.class);
            for (WebEntityDim webEntityDim : webEntityDimList) {
                HdfsWriter.writeLine(webEntityDimWriter, webEntityDim.stringify());
            }
            HdfsWriter.closeWriter(webEntityDimWriter);
        }
    }

    private BufferedWriter getDimWriter(Configuration configuration, String outputFolder, boolean append, DimEntity dimEntity) throws IOException {
        String product = getDimName(dimEntity);
        String outputProductDirectory = outputFolder + File.separator + product;
        return HdfsWriter.createWriter(outputProductDirectory, configuration, append);
    }

    private BufferedWriter getDimWriter(Configuration configuration, String outputFolder, boolean append, Class<?> clazz) throws IOException {
        String product = getDimName(clazz);
        String outputProductDirectory = outputFolder + File.separator + product;
        return HdfsWriter.createWriter(outputProductDirectory, configuration, append);
    }

    public static String getDimName(DimEntity dimEntity) {
        Dim dimMeta = dimEntity.getClass().getAnnotation(Dim.class);
        return dimMeta.name();
    }

    public static String getDimName(Class<?> dimEntity) {
        Dim dimMeta = dimEntity.getAnnotation(Dim.class);
        return dimMeta.name();
    }

    @Getter
    @Setter
    public class ParsedDimsHolder {

        private boolean isDelete;

        private ProductDim productDim;

        private MarketingProductDim marketingProductDim;

        private List<WebEntityDim> webEntityDimList;

        private List<ProductRegionLinkDim> productRegionLinkDimList;

        private List<RegionDim> regionDimList;

        private List<ExternalRegionMappingDim> externalRegionMappingDimList;

        private List<ProductMapDim> productMapDimList;
    }


}
