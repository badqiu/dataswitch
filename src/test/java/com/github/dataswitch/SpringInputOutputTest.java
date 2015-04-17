package com.github.dataswitch;

import java.io.IOException;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.github.dataswitch.input.Input;
import com.github.dataswitch.output.Output;
import com.github.dataswitch.util.InputOutputUtil;

public class SpringInputOutputTest {

	@Test
	public void test() throws IOException {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/test/spring/*.xml");
		copy(context,"fileInput","fileOutput");
	}

	private void copy(ClassPathXmlApplicationContext context, String inputId,String outputId) throws IOException {
		Input input = (Input)context.getBean(inputId);
		Output output = (Output)context.getBean(outputId);
		InputOutputUtil.copy(input,output);
		output.close();
	}
	
}
