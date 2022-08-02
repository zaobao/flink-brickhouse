package brickhouse.flink.utils;

import com.google.gson.*;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JSONUtils {

    private static ThreadLocal<Gson> local = new ThreadLocal<Gson>();

    public static Gson getGson() {
        if (local.get() == null) {
            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.registerTypeAdapter(Date.class, new DateTypeAdapter());
            gsonBuilder.registerTypeAdapter(int.class, new IntDeserializer());
            Gson gson = gsonBuilder.create();
            local.set(gson);
            return gson;
        } else {
            return local.get();
        }
    }

    public static <T> T fromJson(String json, Class<T> cls) {
        return getGson().fromJson(json, cls);
    }

    public static <T> T fromJson(Reader json, Class<T> cls) {
        return getGson().fromJson(json, cls);
    }

    public static <T> T fromJson(Reader json, Type typeOfT) {
        return getGson().fromJson(json, typeOfT);
    }

    public static <T> T fromJson(String json,  Type typeOfT) {
        return getGson().fromJson(json, typeOfT);
    }

    public static String toJson(Object obj) {
        return getGson().toJson(obj);
    }

    public static void toJson(Object obj, Writer writer) {
        getGson().toJson(obj, writer);
    }

    public static class DateTypeAdapter implements JsonSerializer<Date>, JsonDeserializer<Date> {

        private final DateFormat format0 = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");
        private final DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
        private final DateFormat format2 = new SimpleDateFormat("HH:mm:ss");

        @Override
        public Date deserialize(JsonElement json, Type t, JsonDeserializationContext jsc) throws JsonParseException {
            if (!(json instanceof JsonPrimitive)) {
                throw new JsonParseException("The date should be a string value");
            }
            if (json.getAsString().equals("")) {
                return null;
            }
            try {
                return format0.parse(json.getAsString());
            } catch (java.text.ParseException e) {
                try {
                    return format1.parse(json.getAsString());
                } catch (java.text.ParseException e1) {
                    try {
                        return format2.parse(json.getAsString());
                    } catch (java.text.ParseException e2) {
                        throw new JsonParseException("Parse '" + json
                                + "' failed. DateFormat must be 'yyyy-MM-dd' or 'HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss'.");
                    }
                }
            }
        }

        @Override
        public JsonElement serialize(Date date, Type arg1, JsonSerializationContext arg2) {
            String dateString = format0.format(date);
            return new JsonPrimitive(dateString);
        }

    }

    public static class IntDeserializer implements JsonDeserializer<Integer> {

        @Override
        public Integer deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            if (!(json instanceof JsonPrimitive)) {
                throw new JsonParseException("The int value should be a promitive value");
            }
            if (((JsonPrimitive)json).isNumber()) {
                return json.getAsInt();
            }
            if (((JsonPrimitive)json).isString()) {
                String str = json.getAsString();
                return NumberUtils.toInt(str, 0);
            }
            return json.getAsInt();
        }

    }

}
