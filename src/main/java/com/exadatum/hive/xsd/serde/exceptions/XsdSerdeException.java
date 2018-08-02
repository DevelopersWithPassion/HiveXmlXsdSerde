package com.exadatum.hive.xsd.serde.exceptions;

public class XsdSerdeException extends RuntimeException {

    public XsdSerdeException(String message) { super(message); }
    public XsdSerdeException(String message, Throwable cause) { super(message, cause); }
    public XsdSerdeException(Throwable cause) { super(cause); }
}
