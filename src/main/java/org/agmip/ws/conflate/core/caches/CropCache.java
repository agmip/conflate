package org.agmip.ws.conflate.core.caches;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.agmip.ace.lookup.LookupCodes;
import org.agmip.ace.util.JsonFactoryImpl;

import com.fasterxml.jackson.core.JsonGenerator;

public enum CropCache {
    INSTANCE;

    private Map<String, String> crops = new ConcurrentHashMap<String, String>();

    CropCache() {
    }

    public Map<String, String> getCrops() {
        return this.crops;
    }

    public boolean updateCrops(String cropCode) {
        if (! this.crops.containsKey(cropCode)) {
            // Lookup crop code
            String cropName = LookupCodes.lookupCode("crid", cropCode, "common");
            // Store crop code and crop name into crops
            this.crops.put(cropCode, cropName);
            return true;
        } else {
            return false;
        }
    }

    public String serialize() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        JsonGenerator g = JsonFactoryImpl.INSTANCE.getGenerator(bos);
        g.writeStartObject();
        for (Map.Entry<String, String> crop : crops.entrySet()) {
            g.writeStringField(crop.getKey(), crop.getValue());
        }
        g.writeEndObject();
        g.close();
        if (bos.size() == 0) {
            return "";
        } else {
            return new String(bos.toByteArray(), "UTF-8");
        }
    }
}
