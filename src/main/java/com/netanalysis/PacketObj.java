package com.netanalysis;

import com.datatorrent.lib.appdata.schemas.TimeBucket;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent.Aggregator;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by ameya on 15/5/17.
 */

public class PacketObj implements Serializable
{

    public String srcIp;
    public String destIp;
    public String srcMac;
    public String destMac;
    public int size;
    public double usage;
    public long time ;
    public int phash;


    public PacketObj() { }

    public PacketObj(Packet packet,long time)
    {
        EthernetPacket ethernetPacket = packet.get(EthernetPacket.class);
        srcMac = ethernetPacket.getHeader().getSrcAddr().toString();
        destMac = ethernetPacket.getHeader().getDstAddr().toString();

        IpPacket ipPacket = packet.get(IpPacket.class);
        srcIp = ipPacket.getHeader().getSrcAddr().toString();
        destIp = ipPacket.getHeader().getDstAddr().toString();

        size = packet.length();
        usage = size ;
        this.time = time;
        phash = packet.hashCode();
    }

    public String getSrcMac()
    {
        return srcMac;
    }

    public String getDestMac()
    {
        return  destMac;
    }

    public String getSrcIp()
    {
        return srcIp;
    }

    public String getDestIp()
    {
        return destIp;
    }

    public int getSize()
    {
        return size;
    }

    public double getUsage()
    {
        return usage;
    }

    public long getTime()
    {
        return time ;
    }

    public int getPhash()
    {
        return phash;
    }


    public void setSrcIp(String srcIp)
    {
        this.srcIp = srcIp;
    }

    public void setDestIp(String destIp)
    {
        this.destIp = destIp;
    }

    public void setSrcMac(String srcMac)
    {
        this.srcMac = srcMac;
    }

    public void setDestMac(String destMac)
    {
        this.destMac = destMac;
    }

    public void setSize(int size)
    {
        this.size = size;
    }

    public void setUsage(double usage)
    {
        this.usage = usage;
    }

    public void setTime(long time)
    {
        this.time = time;
    }

    public void setPhash(int phash)
    {
        this.phash = phash;
    }

    public static class PacketObjAggregator implements Aggregator<PacketObj,PacketObjAggregateEvent>
    {
        String dimension;
        TimeBucket timeBucket;
        int timeBucketInt;
        TimeUnit time;
        boolean srcIp;
        boolean destIp;
        boolean srcMac;
        boolean destMac;
        boolean size;
        boolean phash = true ;
        int dimensionsDescriptorID;

        public void init(String dimension, int dimensionsDescriptorID)
        {
            String[] attributes = dimension.split(":");
            for (String attribute : attributes)
            {
                String[] keyval = attribute.split("=", 2);
                String key = keyval[0];
                if (key.equals("time"))
                {
                    time = TimeUnit.valueOf(keyval[1]);
                    timeBucket = TimeBucket.TIME_UNIT_TO_TIME_BUCKET.get(time);
                    timeBucketInt = timeBucket.ordinal();
                    time = timeBucket.getTimeUnit();
                }
                else if (key.equals("srcIp"))
                    srcIp = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
                else if (key.equals("destIp"))
                    destIp = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
                else if (key.equals("srcMac"))
                    srcMac = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
                else if (key.equals("destMac"))
                    destMac = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
                else if (key.equals("size"))
                    size = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
                else
                    throw new IllegalArgumentException("Unknown Attribute "+ attribute + "specified as a part of dimension!");
            }

            this.dimensionsDescriptorID = dimensionsDescriptorID;
            this.dimension = dimension;
        }

        public String getDimension()
        {
            return dimension;
        }

        @Override
        public String toString()
        {
            return dimension;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 83 * hash + (this.time != null ? this.time.hashCode() : 0);
            hash = 83 * hash + (this.srcIp ? 1 : 0);
            hash = 83 * hash + (this.destIp ? 1 : 0);
            hash = 83 * hash + (this.srcMac ? 1 : 0);
            hash = 83 * hash + (this.destMac ? 1 : 0);
            hash = 83 * hash + (this.size ? 1 : 0);
            hash = 83 * hash + (this.phash ? 1 : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || !(obj instanceof PacketObjAggregator))
            {
                return false;
            }
            final PacketObjAggregator other = (PacketObjAggregator) obj;
            if (this.time == other.time)
                return true ;
            if (this.srcIp == other.srcIp && this.destIp == other.destIp)
                return true ;
            if (this.srcMac == other.srcMac && this.destMac == other.destMac)
                return true ;

            return false;
        }

        @Override
        public PacketObjAggregateEvent getGroup(PacketObj src, int aggregatorIndex)
        {
            PacketObjAggregateEvent event = new PacketObjAggregateEvent(aggregatorIndex);

            if (time != null)
            {
                event.time = timeBucket.roundDown(src.time);
                event.timeBucket = timeBucketInt ;
            }

            if (srcIp)
                event.srcIp = src.srcIp;
            if (destIp)
                event.destIp = src.destIp;
            if (srcMac)
                event.srcMac = src.srcMac;
            if (destMac)
                event.destMac = src.destMac;
            if (size)
                event.size = src.size;
            if (phash)
                event.phash = src.phash;

            event.aggregatorIndex = aggregatorIndex;
            event.dimensionsDescriptorID = dimensionsDescriptorID;

            return event;
        }

        @Override
        public void aggregate(PacketObjAggregateEvent dest, PacketObj src)
        {
            dest.usage+=(src.usage/1048576); //Packets are in bytes so dividing by 1024*1024 to get the usage in MBs
        }

        @Override
        public void aggregate(PacketObjAggregateEvent dest, PacketObjAggregateEvent src)
        {
            dest.usage+=(src.usage/1048576); //Packets are in bytes so dividing by 1024*1024 to get the usage in MBs
        }


        @Override
        public int hashCode(PacketObj packetObj)
        {
            int hash = 5 ;

            if (phash)
                hash = 71 * hash + packetObj.phash ;
            if (size)
                hash = 71 * hash + packetObj.size ;
            if (time != null)
            {
                long ltime = time.convert(packetObj.time, TimeUnit.MILLISECONDS);
                hash = 71 * hash + (int)(ltime ^ (ltime >>> 32));
            }

            return hash;
        }

        @Override
        public boolean equals(PacketObj p1, PacketObj p2)
        {
            if (p1.getSrcIp().equals(p2.getSrcIp()) && p1.getDestIp().equals(p2.getDestIp()))
                return true;
            if (p1.getSrcMac().equals(p2.getSrcMac()) && p1.getDestMac().equals(p2.getDestMac()))
                return true;

            return false;
        }
    }

    public static class PacketObjAggregateEvent extends PacketObj implements AggregateEvent
    {
        int aggregatorIndex;
        public int timeBucket;
        private int dimensionsDescriptorID;

        public PacketObjAggregateEvent()
        {
            //For Kryo Serialization
        }

        public PacketObjAggregateEvent(int aggregatorIndex)
        {
            this.aggregatorIndex = aggregatorIndex;
        }

        @Override
        public int getAggregatorIndex()
        {
            return aggregatorIndex;
        }

        public void setAggregatorIndex(int aggregatorIndex)
        {
            this.aggregatorIndex = aggregatorIndex;
        }

        public int getDimensionsDescriptorID()
        {
            return dimensionsDescriptorID;
        }

        public void setDimensionsDescriptorID(int dimensionsDescriptorID)
        {
            this.dimensionsDescriptorID = dimensionsDescriptorID;
        }

        @Override
        public int hashCode()
        {
            int hash = 5;
            hash = (71 * hash) + this.phash;
            hash = (71 * hash) + this.size;
            return hash;
        }


        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || !(obj instanceof PacketObjAggregateEvent))
            {
                return false;
            }

            PacketObjAggregateEvent poae = (PacketObjAggregateEvent)obj;

            if (this.srcIp == poae.getSrcIp() && this.destIp == poae.getDestIp())
                return true;
            if (this.srcMac == poae.getSrcMac() && this.destMac == poae.getDestMac())
                return true;

            return false;
        }

        private static final Logger LOG = LoggerFactory.getLogger(PacketObjAggregateEvent.class);
    }

}