package org.example;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

import java.io.IOException;
import java.util.Date;

public class DownloadSampleLine {

    private static String accessId = "ak";
    private static String accessKey = "sk";
    private static String odpsUrl = "http://service.cn-beijing.maxcompute.aliyun.com/api";
    private static String tunnelUrl = "http://dt.cn-beijing.maxcompute.aliyun-inc.com";
    private static String project = "p";
    private static String table = "t";
    private static long limitRows = 10000000;

    public static void main(String[] args) {
        if (args.length > 1) {
            System.out.println("args[1]: " + args[1]);
            limitRows = Long.parseLong(args[1]);
        }
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(project);
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(tunnelUrl);
        try {
            long rowStartTime = System.currentTimeMillis();
            rowReader(tunnel, limitRows);
            System.out.println(System.currentTimeMillis() - rowStartTime);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void rowReader(TableTunnel tunnel, long limit) throws TunnelException, IOException {
        TableTunnel.DownloadSession downloadSession = tunnel.createDownloadSession(project, table);
        System.out.println("Session Status is : " + downloadSession.getStatus().toString());
        System.out.println("RecordCount is: " + downloadSession.getRecordCount());
        RecordReader recordReader = downloadSession.openRecordReader(0, limit);
        Record record;
        while ((record = recordReader.read()) != null) {
            consumeRecord(record, downloadSession.getSchema());
        }
        recordReader.close();
    }

    private static void consumeRecord(Record record, TableSchema schema) {
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumn(i);
            String colValue;
            switch (column.getType()) {
                case BIGINT: {
                    Long v = record.getBigint(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case BOOLEAN: {
                    Boolean v = record.getBoolean(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case DATETIME: {
                    Date v = record.getDatetime(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case DOUBLE: {
                    Double v = record.getDouble(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                case VARCHAR:
                case STRING: {
                    Object v = record.get(i);
                    colValue = v == null ? null : v.toString();
                    break;
                }
                default:
                    throw new RuntimeException("Unknown column type: "
                            + column.getType());
            }
            // System.out.print(colValue == null ? "null" : colValue);
            if (i != schema.getColumns().size()) {
                // System.out.print("\t");
            }
        }
        // System.out.println();
    }
}
