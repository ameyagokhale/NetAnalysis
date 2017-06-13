/**
 * Put your copyright and license info here.
 */
package com.netanalysis;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Map;

@ApplicationAnnotation(name="NetAnalysis")
public class Application implements StreamingApplication
{
    public static final String APP_NAME = "NetAnalysis";
    public static final String EVENT_SCHEMA = "packetSchema.json";

    public String appName = APP_NAME;
    public String eventSchemaLocation = EVENT_SCHEMA;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        String propStorePath = "dt.operator.Store.fileStore.basePathPrefix";

        String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);
        /*
        String jsonSchema = "{\"keys\":\n" +
                "        [{\"name\":\"srcIp\", \"type\":\"string\"},\n" +
                "          {\"name\":\"destIp\", \"type\":\"string\"},\n" +
                "          {\"name\":\"srcMac\", \"type\":\"string\"},\n" +
                "          {\"name\":\"destMac\", \"type\":\"string\"}],\n" +
                "  \"values\":\n" +
                "          [{\"name\":\"size\", \"type\":\"int\", \"aggregators\":[\"SUM\",\"COUNT\",\"AVG\"]},\n" +
                "            {\"name\":\"usage\", \"type\":\"double\", \"aggregators\":[\"SUM\",\"COUNT\",\"AVG\"]}],\n" +
                "  \"timeBuckets\":\n" +
                "          [\"1h\",\"1m\"],\n" +
                "  \"dimensions\":\n" +
                "          [{\"combination\":[]},\n" +
                "            {\"combination\":[\"srcIp\"], \"additionalValues\":[\"size:MIN\", \"usage:MIN\", \"size:MAX\", \"usage:MAX\"]},\n" +
                "            {\"combination\":[\"destIp\"]},\n" +
                "            {\"combination\":[\"srcMac\"], \"additionalValues\":[\"size:MIN\", \"usage:MIN\", \"size:MAX\", \"usage:MAX\"]},\n" +
                "            {\"combination\":[\"destMac\"]},\n" +
                "            {\"combination\":[\"scrIp\", \"destIp\"]},\n" +
                "            {\"combination\":[\"srcMac\", \"destMac\"]},\n" +
                "            {\"combination\":[\"scrIp\", \"time\"]},\n" +
                "            {\"combination\":[\"scrMac\", \"time\"]}]\n" +
                "}\n";
                */
        KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput",KafkaSinglePortInputOperator.class);
        NetworkPacketParser parser = dag.addOperator("parser",NetworkPacketParser.class);

        StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", StringFileOutputOperator.class);

        //DimensionsComputation<PacketObj,PacketObj.PacketObjAggregateEvent> dimensions  = dag.addOperator("Dimensions",new DimensionsComputation<PacketObj, PacketObj.PacketObjAggregateEvent>()) ;

        DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation",DimensionsComputationFlexibleSingleSchemaPOJO.class);
        dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
        dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);
        AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

        Map<String,String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("srcIp","getSrcIp()");
        keyToExpression.put("destIp","getDestIp()");
        keyToExpression.put("srcMac","getSrcMac()");
        keyToExpression.put("destMac","getDestMac()");
        //keyToExpression.put("time","getTime()");

        Map<String,String> aggregateToExpression = Maps.newHashMap();
        aggregateToExpression.put("size","getSize()");
        aggregateToExpression.put("usage","getUsage()");

        dimensions.setKeyToExpression(keyToExpression);
        dimensions.setAggregateToExpression(aggregateToExpression);
        dimensions.setConfigurationSchemaJSON(eventSchema);

        dimensions.setUnifier(new DimensionsComputationUnifierImpl<DimensionsEvent.InputEvent, DimensionsEvent.Aggregate>());
        dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 2048);

        //Set store properties
        String basePath = Preconditions.checkNotNull(conf.get(propStorePath),"add the property in properties.xml");
        basePath += Path.SEPARATOR + System.currentTimeMillis();
        TFileImpl hdsFile = new TFileImpl.DTFileImpl();
        hdsFile.setBasePath(basePath);
        store.setFileStore(hdsFile);
        store.getResultFormatter().setContinuousFormatString("#.00");
        store.setConfigurationSchemaJSON(eventSchema);

        PubSubWebSocketAppDataQuery wsIn = new PubSubWebSocketAppDataQuery();
        store.setEmbeddableQueryInfoProvider(wsIn);

        PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

        //Set remaining dag options


        dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

        dag.addStream("toEncoder", kafkaInput.outputPort, parser.in);
        dag.addStream("toHDFS", parser.outstr, fileOutput.input);
        dag.addStream("toDimensions",parser.out, dimensions.input);
        dag.addStream("toStore",dimensions.output, store.input);
        dag.addStream("toQuery",store.queryResult, wsOut.input);
    }
}