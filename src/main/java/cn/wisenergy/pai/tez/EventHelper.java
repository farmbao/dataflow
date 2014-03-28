package cn.wisenergy.pai.tez;

import java.util.ArrayList;
import java.util.List;

import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import cn.wisenergy.pai.hadoop2.Constants;

public class EventHelper {
	/**
	 * 
	 * @param context
	 * @param i:traceSize
	 */
	public static void sentVertexManagerEvent(TezProcessorContext context, long i) {
		VertexManagerEventPayloadProto proto = VertexManagerEventPayloadProto.getDefaultInstance();
		proto = proto.newBuilderForType().setOutputSize(i * Constants.DEFAULT_DATALEN).build();
		VertexManagerEvent event = new VertexManagerEvent(context.getTaskVertexName(), proto.toByteArray());
		List<Event> events = new ArrayList<Event>();
		events.add(event);
		context.sendEvents(events);
	}

}
