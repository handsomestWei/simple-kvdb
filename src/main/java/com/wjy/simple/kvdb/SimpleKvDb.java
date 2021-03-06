package com.wjy.simple.kvdb;

import lombok.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SimpleKvDb {

    // 数据文件后缀。字节存储
    private static final String DATA_SUFFIX = ".data";
    // 元数据文件后缀。按行存储，格式为key:数据起始位置:数据长度
    private static final String META_SUFFIX = ".meta";
    // 元数据备份文件后缀
    private static final String META_BAK_SUFFIX = ".meta.bak";
    // 分隔符
    private static final String KEY_SPLIT_SYMBOL = ":";

    private String dbName;
    private String dbPath;
    private ConcurrentHashMap<String, MetaData> metaDataMap = new ConcurrentHashMap<>();
    // 使用共享变量实现简单锁。元数据刷盘时，只可读不可写
    private static boolean lockFlag = false;
    // 运行状态。运行true，关闭false
    private boolean status = false;
    // 数据文件操作对象
    private RandomAccessFile raf = null;

    public SimpleKvDb(@NonNull String dbName, @NonNull String dbPath) throws IOException {
        this.dbName = dbName;
        this.dbPath = dbPath;
        loadMetaData(dbName, dbPath);
        loadMetaDataBak(dbName, dbPath);
        loadData(dbName, dbPath);
        runFlushMetaDataTask();
        this.status = true;
    }

    // 继承类需实现对象转字节数组的方法
    public abstract byte[] toBytes(Object data) throws IOException;

    public boolean isRunning() {
        return this.status;
    }

    public byte[] getData(@NonNull String key) throws IOException {
        if (!isRunning()) {
            return null;
        }
        MetaData metaData = metaDataMap.get(key);
        if (metaData == null) {
            return null;
        }
        RandomAccessFile readRaf = new RandomAccessFile(dbPath + dbName + DATA_SUFFIX, "r");
        readRaf.seek(metaData.startIndex);
        byte[] buff = new byte[metaData.dataSize];
        int len;
        while ((len = readRaf.read(buff)) > 0) {
        }
        return buff;
    }

    public boolean setData(@NonNull String key, @NonNull Object data) throws IOException {
        if (!isRunning()) {
            return false;
        }
        isLock();
        MetaData metaData = metaDataMap.get(key);
        if (metaData != null) {
            return updateData(key, data);
        }
        byte[] bytes = toBytes(data);
        return writeDataNewSpace(key, bytes);
    }

    public boolean updateData(@NonNull String key, @NonNull Object data) throws IOException {
        if (!isRunning()) {
            return false;
        }
        isLock();
        MetaData metaData = metaDataMap.get(key);
        if (metaData == null) {
            return false;
        }
        byte[] bytes = toBytes(data);
        // 先尝试覆盖原区域
        boolean result = writeDatCoverSpace(key, bytes);
        if (!result) {
            // 使用新区域
            result = writeDataNewSpace(key, bytes);
        }
        return result;
    }

    public boolean deleteData(@NonNull String key) {
        if (!isRunning()) {
            return false;
        }
        isLock();
        return metaDataMap.remove(key) != null;
    }

    public boolean close() throws IOException {
        if (!isRunning()) {
            return false;
        }
        flushMetaData(dbName, dbPath, metaDataMap);
        this.metaDataMap = new ConcurrentHashMap<>();
        this.raf.close();
        this.status = false;
        return true;
    }

    // 加载元数据
    private void loadMetaData(String dbName, String dbPath) throws IOException {
        File metaFile = new File(dbPath + dbName + META_SUFFIX);
        if (!metaFile.exists()) {
            metaFile.createNewFile();
        }
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(metaFile));) {
            String line = null;
            while((line = bufferedReader.readLine()) != null) {
                loadMetaDataLine(line);
            }
        }
    }

    private void loadMetaDataLine(String line) {
        MetaData metaData = new MetaData();
        String[] temp = line.split(KEY_SPLIT_SYMBOL);
        StringBuffer sb = new StringBuffer();
        for (int i = temp.length - 1; i >= 0; i--) {
            if (i == temp.length - 1) {
                metaData.dataSize = Integer.parseInt(temp[i]);
            } else if (i == temp.length - 2) {
                metaData.startIndex = Long.parseLong(temp[i]);
            } else {
                sb.append(temp[i]).append(KEY_SPLIT_SYMBOL);
            }
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        metaDataMap.put(sb.toString(), metaData);
    }

    private void loadMetaDataBak(String dbName, String dbPath) throws IOException {
        File metaBakDataFile = new File(dbPath + dbName + META_BAK_SUFFIX);
        if (!metaBakDataFile.exists()) {
            return;
        }
        BufferedReader bufferedReader = new BufferedReader(new FileReader(metaBakDataFile));
        String line = null;
        while((line = bufferedReader.readLine()) != null) {
            loadMetaDataLine(line);
        }
    }

    // 加载数据。获取数据文件操作对象
    private void loadData(String dbName, String dbPath) throws IOException {
        File dataFile = new File(dbPath + dbName + DATA_SUFFIX);
        raf = new RandomAccessFile(dataFile, "rw");
    }

    // 存储在运行内存的元数据定时保存到文件
    public static void flushMetaData(String dbName, String dbPath, ConcurrentHashMap<String, MetaData> metaDataMap) throws IOException {
        getLock();
        // 将内存里全量元数据写入新文件
        File metaBakDataFile = new File(dbPath + dbName + META_BAK_SUFFIX);
        if (metaBakDataFile.length() > 0) {
            // 清理旧文件
            metaBakDataFile.deleteOnExit();
        }
        try (FileWriter fw = new FileWriter(metaBakDataFile, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            Set<Map.Entry<String, MetaData>> entrySet = metaDataMap.entrySet();
            for (Map.Entry<String, MetaData> entry : entrySet) {
                bw.write(entry.getKey() + KEY_SPLIT_SYMBOL +
                        entry.getValue().getStartIndex() + KEY_SPLIT_SYMBOL +
                        entry.getValue().getDataSize());
                bw.newLine();
            }
        }
        // 最新数据已备份
        if (metaBakDataFile.length() > 0) {
            // 删除旧文件
            File metaFile = new File(dbPath + dbName + META_SUFFIX);
            metaFile.delete();
            // 对新文件重命名
            metaBakDataFile.renameTo(metaFile);
        }
        releaseLock();
    }

    // 使用新区域写数据
    private boolean writeDataNewSpace(String key, byte[] bytes) throws IOException {
        long startIndex = seekAndWriteFile(bytes);
        metaDataMap.put(key, new MetaData(startIndex, bytes.length));
        return true;
    }

    // 覆盖原区域写数据
    private boolean writeDatCoverSpace(String key, byte[] bytes) throws IOException {
        MetaData metaData = metaDataMap.get(key);
        if (metaData == null) {
            return false;
        }
        if (metaData.dataSize < bytes.length) {
            return false;
        }
        seekAndWriteFile(metaData.startIndex, bytes);
        metaData.dataSize = bytes.length;
        return true;
    }

    private void seekAndWriteFile(long seekIndex, byte[] bytes) throws IOException {
        synchronized (raf) {
            raf.seek(seekIndex);
            raf.write(bytes);
        }
    }

    private long seekAndWriteFile(byte[] bytes) throws IOException {
        synchronized (raf) {
            long startIndex = raf.length();
            raf.seek(startIndex);
            raf.write(bytes);
            return startIndex;
        }
    }

    private void isLock() {
        while (lockFlag == true) {
            // 等待
        }
    }

    private static void getLock() {
        while (lockFlag == true) {
            // 等待获取锁
        }
        lockFlag = true;
    }

    private static void releaseLock() {
        // 释放锁
        lockFlag = false;
    }

    // 定时执行元数据刷盘任务
    private void runFlushMetaDataTask() {
        TimerTask task = new TimerTask() {
            @SneakyThrows
            @Override
            public void run() {
                if (status) {
                    SimpleKvDb.flushMetaData(dbName, dbPath, metaDataMap);
                }
            }
        };
        Timer timer = new Timer();
        // 延迟10秒执行，每120秒执行一次
        timer.schedule(task, 10 * 1000, 120 * 1000);
    }

    @Data
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    private class MetaData {
        // 数据在文件中的起始位置
        private Long startIndex;
        // 数据长度
        private Integer dataSize;
    }

}
