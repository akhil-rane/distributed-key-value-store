package com.cs6650;

public class Abort extends RuntimeException {
	public Abort(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
