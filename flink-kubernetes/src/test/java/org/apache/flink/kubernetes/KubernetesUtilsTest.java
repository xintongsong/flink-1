/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link KubernetesUtils}.
 */
public class KubernetesUtilsTest extends TestLogger {

	@Test
	public void testGetJobManagerShellCommand() {
		final Configuration cfg = new Configuration();

		String confDirInPod = "/opt/flink/conf";
		String logDirInPod = "/opt/flink/log";

		final String java = "$JAVA_HOME/bin/java";
		final String classpath = "-classpath $FLINK_CLASSPATH";
		final String jvmmem = "-Xms768m -Xmx768m";
		final String jvmOpts = "-Djvm"; // if set
		final String jmJvmOpts = "-DjmJvm"; // if set
		final String logfile = String.format("-Dlog.file=%s/jobmanager.log", logDirInPod);
		final String logback = String.format("-Dlogback.configurationFile=file:%s/logback.xml", confDirInPod);
		final String log4j = String.format("-Dlog4j.configuration=file:%s/log4j.properties", confDirInPod);
		final String mainClass = "org.apache.flink.kubernetes.KubernetesUtilsTest";
		final String mainClassArgs = "--job-id=1 -Dtest.key=value";
		final String redirects = String.format("1> %s/jobmanager.out 2> %s/jobmanager.err", logDirInPod, logDirInPod);

		int jobManagerMem = 768;
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + // logging
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					false, false, this.getClass().getName(), mainClassArgs));

		// logback only
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, false, this.getClass().getName(), mainClassArgs));

		// log4j only
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					false, true, this.getClass().getName(), mainClassArgs));

		// logback + log4j
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, true, this.getClass().getName(), mainClassArgs));

		// logback + log4j, different JVM opts
		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + jvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, true, this.getClass().getName(), mainClassArgs));

		// logback + log4j,different TM JVM opts
		cfg.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, true, this.getClass().getName(), mainClassArgs));

		// no args
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + "  " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, true, this.getClass().getName(), null));

		// now try some configurations with different container-start-command-template

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args% %redirects%");
		assertEquals(
			java + " 1 " + classpath + " 2 " + jvmmem +
				" " + jvmOpts + " " + jmJvmOpts + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, true, this.getClass().getName(), mainClassArgs));

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% %jvmmem% %logging% %jvmopts% %class% %args% %redirects%");
		assertEquals(
			java + " " + jvmmem +
				" " + logfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + jmJvmOpts + // jvmOpts
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getJobManagerStartCommand(cfg, jobManagerMem, confDirInPod, logDirInPod,
					true, true, this.getClass().getName(), mainClassArgs));

	}

	@Test
	public void testGetTaskManagerShellCommand() {
		final Configuration cfg = new Configuration();

		String confDirInPod = "/opt/flink/conf";
		String logDirInPod = "/opt/flink/log";

		final String java = "$JAVA_HOME/bin/java";
		final String classpath = "-classpath $FLINK_CLASSPATH";
		final String jvmmem = "-Xms768m -Xmx768m -XX:MaxDirectMemorySize=256m";
		final String jvmOpts = "-Djvm"; // if set
		final String tmJvmOpts = "-DtmJvm"; // if set
		final String logfile = String.format("-Dlog.file=%s/taskmanager.log", logDirInPod);
		final String logback = String.format("-Dlogback.configurationFile=file:%s/logback.xml", confDirInPod);
		final String log4j = String.format("-Dlog4j.configuration=file:%s/log4j.properties", confDirInPod);
		final String mainClass = "org.apache.flink.kubernetes.KubernetesUtilsTest";
		final String mainClassArgs = "-Dtest.key=value";
		final String redirects = String.format("1> %s/taskmanager.out 2> %s/taskmanager.err", logDirInPod, logDirInPod);

		final ContaineredTaskManagerParameters tmParams =
			new ContaineredTaskManagerParameters(1024, 768, 256, 4,
				new HashMap<>());

		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + // logging
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					false, false, this.getClass(), mainClassArgs));

		// logback only
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, false, this.getClass(), mainClassArgs));

		// log4j only
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					false, true, this.getClass(), mainClassArgs));

		// logback + log4j
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, true, this.getClass(), mainClassArgs));

		// logback + log4j, different JVM opts
		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + jvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, true, this.getClass(), mainClassArgs));

		// logback + log4j,different TM JVM opts
		cfg.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + jvmOpts + " " + tmJvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, true, this.getClass(), mainClassArgs));

		// no args
		assertEquals(
			java + " " + classpath + " " + jvmmem +
				" " + jvmOpts + " " + tmJvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + "  " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, true, this.getClass(), null));

		// now try some configurations with different container-start-command-template

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args% %redirects%");
		assertEquals(
			java + " 1 " + classpath + " 2 " + jvmmem +
				" " + jvmOpts + " " + tmJvmOpts + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, true, this.getClass(), mainClassArgs));

		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
			"%java% %jvmmem% %logging% %jvmopts% %class% %redirects%");
		assertEquals(
			java + " " + jvmmem +
				" " + logfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + tmJvmOpts + // jvmOpts
				" " + mainClass + " " + redirects,
			KubernetesUtils
				.getTaskManagerShellCommand(cfg, tmParams, confDirInPod, logDirInPod,
					true, true, this.getClass(), mainClassArgs));

	}
}
