package de.gsi.acc.remote;

public enum MimeType {
    PLAINTEXT("text/plain"),
    BINARY("application/octet-stream"),
    JSON("application/json"),
    XML("text/xml"),
    HTML("text/html"),
    PNG("image/png"),
    EVENT_STREAM("text/event-stream"),
    UNKNOWN("application/octet-stream");

    private final String typeDef;

    MimeType(final String definition) {
        typeDef = definition;
    }

    public String getTypeDef() {
        return typeDef;
    }

    @Override
    public String toString() {
        return typeDef;
    }

    public static MimeType parse(final String text) {
        if (text == null || text.isEmpty() || text.isBlank()) {
            return UNKNOWN;
        }
        for (final MimeType type : MimeType.values()) {
            if (type.getTypeDef().equalsIgnoreCase(text)) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
