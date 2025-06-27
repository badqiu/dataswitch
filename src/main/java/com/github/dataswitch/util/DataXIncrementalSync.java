package com.github.dataswitch.util;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataX增量同步服务（支持任务状态持久化）
 * 功能：
 * 1. 基于任务名称保存最后同步时间
 * 2. 支持动态传入任务名和同步间隔（分钟）
 * 使用示例：java -jar datax-sync.jar --task=user_sync --interval=10
 */
public class DataXIncrementalSync {

    // 任务状态存储目录（格式：/data/state/{taskName}.txt）
    private static final String STATE_DIR = "/data/dw_etl_state/";
    private static final Map<String, Long> stateCache = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        Map<String, String> params = parseArgs(args);
        String taskName = params.getOrDefault("task", "default_task");
        int interval = Integer.parseInt(params.getOrDefault("interval", "10"));

        // 执行增量同步
        long startTime = getLastSyncTime(taskName);
        long endTime = System.currentTimeMillis() / 1000; // 当前时间戳（秒）
//        runDataXJob(taskName, startTime, endTime);
        saveLastSyncTime(taskName, endTime); // 保存状态
    }



    /**
     * 任务状态管理（文件持久化）
     */
    private static long getLastSyncTime(String taskName) throws IOException {
        // 优先从缓存读取
        if (stateCache.containsKey(taskName)) {
            return stateCache.get(taskName);
        }

        // 从文件加载状态
        Path stateFile = Paths.get(STATE_DIR + taskName + ".txt");
        if (Files.exists(stateFile)) {
            String timeStr = new String(Files.readAllBytes(stateFile)).trim();
            return Long.parseLong(timeStr);
        }
        return System.currentTimeMillis() / 1000 - 86400; // 默认24小时前
    }

    private static void saveLastSyncTime(String taskName, long time) throws IOException {
        Path stateFile = Paths.get(STATE_DIR + taskName + ".txt");
        Files.createDirectories(stateFile.getParent());
        Files.write(stateFile, String.valueOf(time).getBytes());
        stateCache.put(taskName, time); // 更新缓存
    }

    /**
     * 命令行参数解析
     */
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] kv = arg.substring(2).split("=");
                if (kv.length == 2) {
                    params.put(kv[0], kv[1]);
                }
            }
        }
        return params;
    }
}