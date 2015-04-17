package com.github.dataswitch.serializer;

import java.io.Flushable;

public interface Serializer<T> extends org.springframework.core.serializer.Serializer<T>,Flushable{
}
