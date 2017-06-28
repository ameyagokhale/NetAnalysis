package com.netanalysis;

import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.Packet;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by ameya on 15/5/17.
 */

public class PacketObj implements Serializable
{
    static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Application.class);

    public String srcIp;
    public String destIp;
    public String srcMac;
    public String destMac;
    public int size;
    public double usage;
    public long time ;

    public PacketObj() { }

    public PacketObj(Packet packet,long time)
    {
        EthernetPacket ethernetPacket = packet.get(EthernetPacket.class);
        IpPacket ipPacket = packet.get(IpPacket.class);

        if (ethernetPacket != null && ipPacket != null)
        {
            srcMac = ethernetPacket.getHeader().getSrcAddr().toString();
            destMac = ethernetPacket.getHeader().getDstAddr().toString();
            srcIp = ipPacket.getHeader().getSrcAddr().toString();
            destIp = ipPacket.getHeader().getDstAddr().toString();
            size = packet.length();
            usage = size ;
            this.time = time;
        }
        else
            size = 0;
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
}