package com.netanalysis;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.factory.PacketFactories;
import org.pcap4j.packet.namednumber.DataLinkType;

/**
 * Created by ameya on 13/4/17.
 */
public class NetworkPacketParser extends BaseOperator
{
    private String eventSchemaJSON;
    private transient DimensionalConfigurationSchema schema;

    public final transient DefaultOutputPort<String> outstr = new DefaultOutputPort<>();

    public final transient DefaultOutputPort<PacketObj> out = new DefaultOutputPort<>();

    public final transient DefaultInputPort<byte[]> in = new DefaultInputPort<byte[]>()
    {
        @Override
        public void process(byte[] tuple) {
            processTuple(tuple);
        }
    };

    void processTuple(byte[] tuple)
    {
        long time;
        if(tuple!=null)
        {
            Packet packet = PacketFactories.getFactory(Packet.class, DataLinkType.class).newInstance(tuple, 0, tuple.length, DataLinkType.EN10MB);
            time = System.currentTimeMillis() ;
            //If it's not an Ethernet Packet
            if (packet == null)
            {
                packet = PacketFactories.getFactory(Packet.class, DataLinkType.class)
                        .newInstance(tuple, 0, tuple.length, DataLinkType.IEEE802);
                time = System.currentTimeMillis();
            }
            PacketObj packetObj = new PacketObj(packet,time);
            outstr.emit(packet.toString());
            out.emit(packetObj);
        }
    }
}