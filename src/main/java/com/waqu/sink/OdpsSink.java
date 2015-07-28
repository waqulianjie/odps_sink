package com.waqu.sink;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.StreamClient;
import com.aliyun.odps.tunnel.StreamClient.ShardState;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamRecordPack;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.google.common.collect.Lists;

public class OdpsSink extends AbstractSink implements Configurable {

	private TableTunnel tableTunnel;
	private Table table;
	private String currentPartition = "";
	private final DateFormat totalDf = new SimpleDateFormat("yyyy-MM-dd-HH");
	private final DateFormat dateDf = new SimpleDateFormat("yyyy-MM-dd");
	private final DateFormat hourDf = new SimpleDateFormat("HH");
	private int bathSize = 10;
	private StreamWriter writers[];
	private StreamClient streamClient;
	private PartitionSpec partitionsSpec;
	private Column[] columns;
	private Random random = new Random();

	@Override
	public void configure(Context context) {

		String accessId = context.getString("sink.odps.access_id", "####");
		String accessKey = context.getString("sink.odps.access_key", "######");
		String endPoint = context.getString("sink.odps.end_point", "http://odps-ext.aliyun-inc.com/api");
		String tunnelEndPoint = context.getString("sink.tunnel.end_point", "http://dh-ext.odps.aliyun-inc.com");
		String tableName = context.getString("sink.table", "####");
		String project = context.getString("sink.project", "######");

		Odps odps = new Odps(new AliyunAccount(accessId, accessKey));
		odps.setEndpoint(endPoint);
		odps.setDefaultProject(project);
		tableTunnel = new TableTunnel(odps);
		tableTunnel.setEndpoint(tunnelEndPoint);

		try {
			streamClient = tableTunnel.createStreamClient(project, tableName);
			streamClient.loadShard(5);
		} catch (TunnelException e) {
			throw new RuntimeException("createStreamClient create error", e);
		}

		table = odps.tables().get(tableName);

		bathSize = context.getInteger("sink.batchSize", 10);
		
		TableSchema tableSchema = streamClient.getStreamSchema();
		columns = (Column[]) tableSchema.getColumns().toArray(new Column[tableSchema.getColumns().size()]);

	}

	private Set<String> getAllPartitions() {
		Set<String> partitionSet = new HashSet<String>();

		List<Partition> list = table.getPartitions();
		for (Partition partition : list) {
			PartitionSpec ps = partition.getPartitionSpec();
			String ctime = ps.get("ctime");
			String hour = ps.get("hour");
			partitionSet.add(ctime + "-" + hour);
		}
		return partitionSet;
	}

	private void addPartition(String ctime, String hour) throws OdpsException {
		table.createPartition(genPartitionSpec(ctime, hour));
	}

	private PartitionSpec genPartitionSpec(String ctime, String hour) {
		PartitionSpec ps = new PartitionSpec();
		ps.set("ctime", ctime);
		ps.set("hour", hour);
		return ps;
	}

	private StreamWriter[] getWriters(String serverTime) throws Exception {
		long now = Long.parseLong(serverTime);
		Date date = new Date();
		date.setTime(now);
		String currentPar = totalDf.format(date);
		if (writers == null || writers.length==0 || !StringUtils.equals(currentPar, currentPartition)) {
			String ctime = dateDf.format(date);
			String hour = hourDf.format(date);
			System.out.println("init new stream writers for hour:" + hour);
			Set<String> partitionSet = getAllPartitions();
			if (!partitionSet.contains(currentPar)) {
				addPartition(ctime, hour);
			}
			freshPartitionSpec(ctime, hour);
			buildStreamWriter();
			currentPartition = currentPar;
		}
		return writers;
	}

	private void freshPartitionSpec(String ctime, String hour) {
		this.partitionsSpec = genPartitionSpec(ctime, hour);
		
	}

	private void buildStreamWriter() throws Exception {
		HashMap<Long, ShardState> shardMap = streamClient.getShardStatus();
		List<Long> shardIdList = new ArrayList<Long>();
		int i = 0;
		for(Entry<Long, ShardState> entry:shardMap.entrySet()){
			if(StringUtils.equals(entry.getValue().name(), StreamClient.ShardState.LOADED.name())){
				shardIdList.add(entry.getKey());
				i++;
			}
			if(i>=10){
				break;
			}
		}
		
		if (shardIdList.size() > 0) {
			writers = new StreamWriter[shardIdList.size()];
			for (int j = 0; j < shardIdList.size() ; j++)
				writers[j] = streamClient.openStreamWriter(((Long) shardIdList.get(j)).longValue());

		} else {
			throw new RuntimeException("OdpsUploadProcessor buildStreamWriter() error, have not loaded shards.");
		}
	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			StreamWriter writers[] = null;
			List<Record> recordList = Lists.newArrayList();
			for (int i = 0; i < bathSize; i++) {
				Event event = ch.take();
				String data = new String(event.getBody(), "UTF-8");
				if (StringUtils.isNotEmpty(data)) {
					String[] lines = StringUtils.split(data, "\n");
//					System.out.println("data:"+data);
					for (String line : lines) {
						String[] cols = StringUtils.splitPreserveAllTokens(line, "\t");
						if (cols.length < 7) {
//							System.out.println("data size error now:"+cols.length+" line:"+line);
							continue;
						}
						if (recordList.size() == 0) {
							writers = this.getWriters(cols[1]);
//							System.out.println("writers.length:"+writers.length);
							// ntype\tserver_time\t ..
						}
						Record record =  new ArrayRecord(columns);
						for (int k = 0; k < columns.length; k++) {
							record.setString(k, cols[k]);
						}
						recordList.add(record);
					}
				}
			}
			StreamRecordPack recordPack = new StreamRecordPack(streamClient.getStreamSchema());
			for(Record record:recordList){
				recordPack.append(record);
			}
//			System.out.println("recordPack.getRecordCount:"+recordPack.getRecordCount());
//			System.out.println("partitionsSpec.toString:"+partitionsSpec.toString());
			writers[random.nextInt(writers.length)].write(partitionsSpec, recordPack);
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();
		}
		return status;
	}
}
