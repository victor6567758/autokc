package com.zv;

import com.zv.config.ZvMonitorConfig;

import java.util.HashMap;
import java.util.Map;

public class ZvMonitorApp {

    public static void main(String[] args) throws Exception {
        ZvMonitorService monitor = new ZvMonitorService(ZvMonitorConfig.load(cliOverrides(args)));
        monitor.start();
        monitor.await();
    }

    /** CLI args use the same keys as the env layer, {@code KEY=value}, and win over it. */
    private static Map<String, String> cliOverrides(String[] args) {
        Map<String, String> overrides = new HashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq > 0) {
                overrides.put(arg.substring(0, eq), arg.substring(eq + 1));
            }
        }
        return overrides;
    }
}
