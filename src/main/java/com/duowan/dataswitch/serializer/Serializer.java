package com.duowan.dataswitch.serializer;

import java.io.Flushable;

public interface Serializer<T> extends org.springframework.core.serializer.Serializer<T>,Flushable{
}
