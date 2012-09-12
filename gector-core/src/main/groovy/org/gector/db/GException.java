package org.gector.db;

public class GException extends RuntimeException {
    
    static final long serialVersionUID = 1L;

    GException( String message ) {
        super( message );
    }
    GException( String message, Throwable cause ) {
        super( message, cause );
    }
}
