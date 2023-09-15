package com.github.dataswitch.enums;

public enum RowKind {
	
    INSERT("+I", (byte) 1),

    UPDATE("+U", (byte) 2),

    DELETE("-D", (byte) 3),
	
	UPSERT("+R",(byte)4);

    private final String shortString;

    private final byte value;

    /**
     * Creates a {@link RowKind} enum with the given short string and byte value representation of
     * the {@link RowKind}.
     */
    RowKind(String shortString, byte value) {
        this.shortString = shortString;
        this.value = value;
    }

    /**
     * Returns a short string representation of this {@link RowKind}.
     *
     * <p>
     *
     * <ul>
     *   <li>"+I" represents {@link #INSERT}.
     *   <li>"+U" represents {@link #UPDATE}.
     *   <li>"-D" represents {@link #DELETE}.
     * </ul>
     */
    public String shortString() {
        return shortString;
    }

    public byte toByteValue() {
        return value;
    }

    /**
     * Creates a {@link RowKind} from the given byte value. Each {@link RowKind} has a byte value
     * representation.
     *
     * @see #toByteValue() for mapping of byte value and {@link RowKind}.
     */
    public static RowKind fromByteValue(byte value) {
        switch (value) {
            case 1:
                return INSERT;
            case 2:
                return UPDATE;
            case 3:
                return DELETE;
            case 4:
                return UPSERT;                
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for row kind.");
        }
    }
}
