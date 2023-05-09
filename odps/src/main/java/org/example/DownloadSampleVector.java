package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class DownloadSampleVector {
    private static String accessId = "ak";
    private static String accessKey = "sk";
    private static String odpsUrl = "http://service.cn-beijing.maxcompute.aliyun.com/api";
    private static String tunnelUrl = "http://dt.cn-beijing.maxcompute.aliyun-inc.com";
    private static String project = "p";
    private static String table = "t";

    private static int threadNum = 10;
    private static long limitRows = 100000;
    private static boolean useSingleThread = false;

    private static ExecutorService pool;

    public static void main(String[] args) {
        if (args.length > 1) {
            System.out.println("args[1]: " + args[1]);
            limitRows = Long.parseLong(args[1]);
        }
        if (args.length > 2) {
            System.out.println("args[2]: " + args[2]);
            threadNum = Integer.parseInt(args[2]);
            if (threadNum <= 1) {
                useSingleThread = true;
            }
        }
        System.out.println("threads: " + threadNum);
        System.out.println("useSingleThread: " + useSingleThread);
        if (!useSingleThread) {
            pool = Executors.newFixedThreadPool(threadNum);
        }
        System.out.println("lines: " + limitRows);

        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(project);
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(tunnelUrl);
        try {
            // col store
            List<Column> columns = getColumns();
            long colStartTime = System.currentTimeMillis();
            if (!useSingleThread) {
                threadColumnReader(tunnel, columns, limitRows);
                System.out.println("Cost Time: " + (System.currentTimeMillis() - colStartTime));
                pool.shutdown();
            } else {
                columnReader(tunnel, columns, limitRows);
                System.out.println("Cost Time: " + (System.currentTimeMillis() - colStartTime));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Column> getColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("lo_orderkey", TypeInfoFactory.BIGINT));
        columns.add(new Column("lo_suppkey", TypeInfoFactory.BIGINT));
        return columns;
    }

    private static void threadColumnReader(TableTunnel tunnel, List<Column> columns, long limit) throws TunnelException, IOException, InterruptedException, ExecutionException {
        DownloadSession dc = tunnel.createDownloadSession(project, table);
        System.out.println("Session Status is : " + dc.getStatus().toString());
        System.out.println("RecordCount is: " + dc.getRecordCount());
        List<Future<Integer>> futures = new ArrayList<>();
        for (Column column : columns) {
            ArrowRecordReader recordReader = dc.openArrowRecordReader(0, limit, ImmutableList.of(column));
            futures.add(pool.submit(() -> {
                VectorSchemaRoot col;
                while ((col = recordReader.read()) != null) {
                    consumeVector(col, dc.getSchema());
                }
                recordReader.close();
                return 0;
            }));
        }

        for (Future<Integer> future : futures) {
            future.get();
        }
    }

    private static void columnReader(TableTunnel tunnel, List<Column> columns, long limit) throws TunnelException, IOException {
        DownloadSession downloadSession = tunnel.createDownloadSession(project, table);
        System.out.println("Session Status is : " + downloadSession.getStatus().toString());
        System.out.println("RecordCount is: " + downloadSession.getRecordCount());
        ArrowRecordReader recordReader = downloadSession.openArrowRecordReader(0, limit, columns);
        VectorSchemaRoot col;
        while ((col = recordReader.read()) != null) {
            consumeVector(col, downloadSession.getSchema());
        }
        recordReader.close();
    }

    private static void consumeVector(VectorSchemaRoot record, TableSchema schema) {
        List<FieldVector> fieldVectors = record.getFieldVectors();
        for (int i = 0; i < fieldVectors.size(); i++) {
            FieldVector column = fieldVectors.get(i);
            ArrowBuf buffers = column.getDataBuffer();
            for (int j = 0; j < column.getValueCount(); j++) {
                Column columnSchema = schema.getColumn(column.getName());
                String colValue = null;
                switch (columnSchema.getTypeInfo().getOdpsType()) {
                    case BIGINT: {
                        long v = buffers.getLong((long) j * Long.BYTES);
                        colValue = Long.toString(v);
                        break;
                    }
                    case STRING:
                    case VARCHAR: {
                        break;
                    }
                }
                // System.out.print(colValue == null ? "null" : colValue);
                if (i != schema.getColumns().size()) {
                    // System.out.print("\t");
                }
            }
            // System.out.println();
        }
    }
}