package com.github.dataswitch.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.input.Input;
import com.github.dataswitch.input.MultiInput;
import com.github.dataswitch.output.BufferedOutput;
import com.github.dataswitch.output.MultiOutput;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.processor.MultiProcessor;
import com.github.dataswitch.processor.Processor;

/**
 * 输入输出类，一个输入可以配置多个输出
 * 
 * 数据处理流程:
 * 
 * Input => Processor => Output
 * 
 * @author badqiu
 *
 */
@Deprecated
public class InputsOutputs extends com.github.dataswitch.InputsOutputs {
}
