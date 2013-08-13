package org.agmip.ws.conflate.core;

public class ConflateFunctions {
    private ConflateFunctions() {
    }

    public static String JSONPWrap(String callback, String data) {
        StringBuilder sb = new StringBuilder();
        sb.append(callback);
        sb.append("(");
        sb.append(data);
        sb.append(");");
        return sb.toString();
    }
}
