package brickhouse.flink.utils;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class JSONUtils {

    private static ThreadLocal<Gson> local = new ThreadLocal<Gson>();

    public static Gson getGson() {
        if (local.get() == null) {
            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.registerTypeAdapter(Date.class, new DateTypeAdapter());
            gsonBuilder.registerTypeAdapter(int.class, new IntDeserializer());
            gsonBuilder.registerTypeAdapter(Instant.class, new InstantTypeAdapter());
            gsonBuilder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeTypeAdapter());
            gsonBuilder.registerTypeAdapter(TimestampData.class, new TimestampDataTypeAdapter());
            gsonBuilder.registerTypeAdapter(Row.class, new RowTypeAdapter());
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

    public static List<String> splitJsonArray(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        List<String> result = new LinkedList<>();
        Gson gson = getGson();
        try (JsonReader jsonReader = new JsonReader(new StringReader(json))) {
            if (jsonReader.peek() == JsonToken.NULL) {
                return null;
            }
            jsonReader.beginArray();
            while (jsonReader.hasNext()) {
                result.add(gson.fromJson(jsonReader, JsonElement.class).toString());
            }
            jsonReader.endArray();
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public static class InstantTypeAdapter implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
        private final DateTimeFormatter format0 = DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
        private final DateTimeFormatter format1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault());
        private final DateTimeFormatter format2 = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());

        @Override
        public Instant deserialize(JsonElement json, Type t, JsonDeserializationContext jsc) throws JsonParseException {
            if (!(json instanceof JsonPrimitive)) {
                throw new JsonParseException("The LocalDateTime value should be a string value");
            }
            if (json.getAsString().equals("")) {
                return null;
            }
            try {
                return Instant.from(format0.parse(json.getAsString()));
            } catch (DateTimeParseException e) {
                try {
                    return Instant.from(format1.parse(json.getAsString()));
                } catch (DateTimeParseException  e1) {
                    try {
                        return Instant.from(format2.parse(json.getAsString()));
                    } catch (DateTimeParseException  e2) {
                        throw new JsonParseException("Parse '" + json
                                + "' failed. Instant must be 'yyyy-MM-dd' or 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'.");
                    }
                }
            }
        }

        @Override
        public JsonElement serialize(Instant instant, Type arg1, JsonSerializationContext arg2) {
            String dateString = format0.format(instant);
            return new JsonPrimitive(dateString);
        }
    }

    public static class LocalDateTimeTypeAdapter implements JsonSerializer<LocalDateTime>, JsonDeserializer<LocalDateTime> {

        private final DateTimeFormatter format0 = DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss.SSS");
        private final DateTimeFormatter format1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private final DateTimeFormatter format2 = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        @Override
        public LocalDateTime deserialize(JsonElement json, Type t, JsonDeserializationContext jsc) throws JsonParseException {
            if (!(json instanceof JsonPrimitive)) {
                throw new JsonParseException("The LocalDateTime value should be a string value");
            }
            if (json.getAsString().equals("")) {
                return null;
            }
            try {
                return LocalDateTime.parse(json.getAsString(), format0);
            } catch (DateTimeParseException e) {
                try {
                    return LocalDateTime.parse(json.getAsString(), format1);
                } catch (DateTimeParseException  e1) {
                    try {

                        return LocalDateTime.parse(json.getAsString(), format2);
                    } catch (DateTimeParseException  e2) {
                        throw new JsonParseException("Parse '" + json
                                + "' failed. LocalDateTime must be 'yyyy-MM-dd' or 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'.");
                    }
                }
            }
        }

        @Override
        public JsonElement serialize(LocalDateTime localDateTime, Type arg1, JsonSerializationContext arg2) {
            String dateString = localDateTime.format(format0);
            return new JsonPrimitive(dateString);
        }

    }

    public static class TimestampDataTypeAdapter implements JsonSerializer<TimestampData>, JsonDeserializer<TimestampData> {

        private final DateTimeFormatter format0 = DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss");
        private final DateTimeFormatter format1 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        private final DateTimeFormatter format2 = DateTimeFormatter.ofPattern("HH:mm:ss");

        @Override
        public TimestampData deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
            if (!(json instanceof JsonPrimitive)) {
                throw new JsonParseException("The TimestampData value should be a string value");
            }
            if (json.getAsString().equals("")) {
                return null;
            }
            return TimestampData.fromLocalDateTime(context.deserialize(json, LocalDateTime.class));
        }

        @Override
        public JsonElement serialize(TimestampData timestampData, Type type, JsonSerializationContext context) {
            return context.serialize(timestampData.toLocalDateTime());
        }

    }

    public static class RowTypeAdapter implements JsonSerializer<Row> {

        @Override
        public JsonElement serialize(Row row, Type type, JsonSerializationContext context) {
            Set<String> fieldNames = row.getFieldNames(true);
            if (fieldNames != null) {
                JsonObject obj = new JsonObject();
                for (String fieldName : fieldNames) {
                    obj.add(fieldName, context.serialize(row.getField(fieldName)));
                }
                return obj;
            }
            int length = row.getArity();
            JsonArray array = new JsonArray(length);
            for (int i = 0; i < length; i++) {
                array.add(context.serialize(row.getField(i)));
            }
            return array;
        }

    }

}
