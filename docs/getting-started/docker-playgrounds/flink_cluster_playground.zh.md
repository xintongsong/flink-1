---
title: "Flink集群场景"
nav-title: 'Flink集群场景'
nav-parent_id: docker-playgrounds
nav-pos: 1
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

在多环境下部署和操作Apache Flink可以有很多种方式，抛开这种多样性而言，Flink集群的基本构建方式和操作原则仍然是相同的。

在这篇文章里，你将会学习如何管理和运行Flink任务，了解如何部署和监控应用程序、Flink如何从失败作业中进行恢复，同时你还会学习如何执行一些日常操作任务，如升级和扩容。

* This will be replaced by the TOC
{:toc}

## 场景说明

这篇文章中的所有操作都是基于一个 
[Flink Session Cluster]({{ site.baseurl }}/concepts/glossary.html#flink-session-cluster) 和一个Kafka集群进行的，
我们会在下文带领大家一起搭建这两个集群。

一个Flink集群总是包含一个 
[Flink Master]({{ site.baseurl }}/concepts/glossary.html#flink-master) 以及一个或多个 
[Flink TaskManager]({{ site.baseurl }}/concepts/glossary.html#flink-taskmanager)。Flink Master 
负责处理 [Job]({{ site.baseurl }}/concepts/glossary.html#flink-job) 提交、
Job监控以及资源管理。Flink TaskManager 运行worker进程，
负责实际任务 
[Tasks]({{ site.baseurl }}/concepts/glossary.html#task) 的执行，而这些任务共同组成了一个 Flink Job。 在这篇文章中，
我们会先运行一个 TaskManager，接下来会扩容到多个 TaskManager。 
另外，这里我们会使用一个 *客户端* 容器（已废弃）来提交Flink Job，
后续还会使用该容器执行一些操作任务。需要注意的是，Flink集群的运行并不需要依赖 *客户端* 容器，
我们这里引入只是为了使用方便。

这里的Kafka集群由一个Zookeeper服务端和一个Kafka Broker组成。

<img src="{{ site.baseurl }}/fig/flink-docker-playground.svg" alt="Flink Docker 场景"
class="offset" width="80%" />

一开始，我们会往 Flink Master 提交一个名为 *Flink事件计数* 的Job，此外，我们还创建了两个Kafka Topic：*input* 和 *output*。

<img src="{{ site.baseurl }}/fig/click-event-count-example.svg" alt="Click Event Count Example"
class="offset" width="80%" />

该Job负责从 *input* topic消费点击事件 `ClickEvent`，每个点击事件都包含一个 `timestamp` 和一个 `page` 属性。
这些事件将按照 `page` 属性进行分组，然后按照每15s窗口 [windows]({{ site.baseurl }}/dev/stream/operators/windows.html) 进行统计，
最终结果输出到 *output* topic中。

总共有6种不同的page属性，针对特定page，我们会按照每15s产生1000个点击事件的速率生成数据。
因此，针对特定page，该Flink job应该能在每个窗口中输出1000个该page的点击数据。

{% top %}

## 环境搭建

{% if site.version contains "SNAPSHOT" %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
  <b>注意</b>: 本文中使用的Apache Flink Docker镜像仅适用于Apache Flink发行版。
  由于你目前正在浏览快照版的文档，因此下文中引用的分支可能不存在。
  你可以手动更改它，也可以通过版本选择器切换到发行版文档再查看。
</p>
{% endif %}

环境搭建只需要几步就可以完成，我们将会带你过一遍必要的操作命令，
并说明如何验证我们正在操作的一切都是运行正常的。

你需要在自己的主机上提前安装好 [docker](https://docs.docker.com/) (1.12+) 和 
[docker-compose](https://docs.docker.com/compose/) (2.1+)。

我们所使用的配置文件位于 
[flink-playgrounds](https://github.com/apache/flink-playgrounds) 仓库中，
检出该仓库并启动docker环境：

{% highlight bash %}
git clone --branch release-{{ site.version }} git@github.com:apache/flink-playgrounds.git
cd flink-cluster-playground
docker-compose up -d
{% endhighlight %}

接下来执行 `docker-compose ps` 命令，输出如下:

{% highlight bash %}
                     Name                                    Command               State                   Ports                
--------------------------------------------------------------------------------------------------------------------------------
flink-cluster-playground_clickevent-generator_1   /docker-entrypoint.sh java ...   Up       6123/tcp, 8081/tcp                  
flink-cluster-playground_client_1                 /docker-entrypoint.sh flin ...   Exit 0                                       
flink-cluster-playground_jobmanager_1             /docker-entrypoint.sh jobm ...   Up       6123/tcp, 0.0.0.0:8081->8081/tcp    
flink-cluster-playground_kafka_1                  start-kafka.sh                   Up       0.0.0.0:9094->9094/tcp              
flink-cluster-playground_taskmanager_1            /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp                  
flink-cluster-playground_zookeeper_1              /bin/sh -c /usr/sbin/sshd  ...   Up       2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
{% endhighlight %}

从上面的信息可以看出客户端容器（client_1）已成功提交了Flink Job ("Exit 0")，
同时包含数据生成器（clickevent-generator_1）在内的所有集群组件都处于运行中状态 ("Up")。

你可以通过执行 `docker-compose down -v` 命令停止docker环境。

## 环境讲解

在这个搭建好的环境中你可以尝试和验证很多事情，在下面的两个部分中我们将向你展示如何与Flink集群进行交互，演示并讲解Flink的一些核心特性。

### Flink UI界面

观察Flink集群首先想到的就是Flink UI界面：打开浏览器并访问 
http://localhost:8081， 如果一切正常，你将会在界面上看到一个TaskManager
和一个处于"RUNNING"状态的名为 *Click Event Count* 的Job。

<img src="{{ site.baseurl }}/fig/playground-webui.png" alt="Playground Flink WebUI"
class="offset" width="100%" />

Flink UI界面包含许多关于Flink集群和运行在其上的Jobs的有用信息，比如：JobGraph, Metrics, Checkpointing Statistics, TaskManager Status等等。 

### 日志

**JobManager**

JobManager日志可以通过 `docker-compose` 命令进行查看。
{% highlight bash %}
docker-compose logs -f jobmanager
{% endhighlight %}

JobManager刚启动完成之时，你会看到很多关于 checkpoint completion （检查点完成）的日志。

**TaskManager**

TaskManager日志也可以通过同样的方式进行查看。
{% highlight bash %}
docker-compose logs -f taskmanager
{% endhighlight %}

TaskManager刚启动完成之时，你同样会看到很多关于 checkpoint completion （检查点完成）的日志。

### Flink CLI

[Flink CLI]({{ site.baseurl }}/ops/cli.html) 相关命令可以在客户端容器内进行使用。
比如，想查看Flink CLI的 `help` 命令，可以通过如下方式进行查看：
{% highlight bash%}
docker-compose run --no-deps client flink --help
{% endhighlight %}

### Flink REST API

[Flink REST API]({{ site.baseurl }}/monitoring/rest_api.html#api) 可以通过本机的 
`localhost:8081` 进行访问，也可以在客户端容器中通过 `jobmanager:8081` 进行访问。
比如，通过如下命令可以获取所有正在运行中的Job：
{% highlight bash%}
curl localhost:8081/jobs
{% endhighlight %}

{% if site.version contains "SNAPSHOT" %}
<p style="border-radius: 5px; padding: 5px" class="bg-info">
  <b>注意</b>: 如果你的主机上没有 `curl` 命令，那么你可以通过客户端容器进行访问（类似于Flink CLI命令）：
  {% highlight bash%}
  docker-compose run --no-deps client curl jobmanager:8081/jobs 
  {% endhighlight %}  
</p>
{% endif %}

### Kafka Topics

要手动查看Kafka Topics中的记录，可以运行如下命令：
{% highlight bash%}
//input topic (1000 records/s)
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input
//output topic (24 records/min)
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
{% endhighlight %}

{%  top %}

## 核心特性探索

在上述所搭建好的环境基础上，本节将对Flink的一些典型操作以及核心特性进行讲解。
本节中的各部分命令不需要按任何特定的顺序执行，这些命令大部分都可以通过[CLI](#Flink-CLI) 或 [RESTAPI](#Flink-REST-API)执行。

### 获取所有运行中的Job

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**命令**
{% highlight bash %}
docker-compose run --no-deps client flink list
{% endhighlight %}
**预期输出**
{% highlight plain %}
Waiting for response...
------------------ Running/Restarting Jobs -------------------
16.07.2019 16:37:55 : <job-id> : Click Event Count (RUNNING)
--------------------------------------------------------------
No scheduled jobs.
{% endhighlight %}
</div>
<div data-lang="REST API" markdown="1">
**请求**
{% highlight bash %}
curl localhost:8081/jobs
{% endhighlight %}
**预期响应 (结果已格式化)**
{% highlight bash %}
{
  "jobs": [
    {
      "id": "<job-id>",
      "status": "RUNNING"
    }
  ]
}
{% endhighlight %}
</div>
</div>

Job一旦提交，就会默认为其生成一个JobID，后续对该Job的
所有操作（无论是通过CLI还是REST API）都需要带上JobID。

### Job失败与恢复

在Job(部分)失败的情况下，Flink对事件处理依然能够提供"exactly-once"的保障，
在本节中你将会观察到并能够在某种程度上验证这种行为。 

#### Step 1: 观察输出

如[前文](#场景说明)所述，事件以特定速率生成，刚好使得每个统计窗口都包含确切的1000条记录。
因此，你可以实时查看output topic的输出，确定失败恢复后所有的窗口依然输出正确的统计数字，
以此来验证Flink在TaskManager失败时能够成功恢复，而且不丢失数据、不产生数据重复。

为此，通过控制台命令消费 *output* topic，保持消费直到Job从失败中恢复(Step 3)。

{% highlight bash%}
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
{% endhighlight %}

#### Step 2: 模拟失败

为了模拟部分失败故障，你可以kill掉一个TaskManager，这种失败行为在生产环境中就相当于
TaskManager进程挂掉、TaskManager机器宕机，或者从框架或用户代码中抛出的一个临时异常（例如，由于外部资源暂时不可用）而导致的失败。   

{% highlight bash%}
docker-compose kill taskmanager
{% endhighlight %}

几秒钟后，你将在FLink UI界面中看到这个Job失败，并且又自动重新提交了。
此时，可能由于缺少资源（比如没有可用的TaskSlots）导致Job重启失败，
该Job将经历一个取消、重新提交不断循环的周期，直到资源再次可用时该Job会重启成功。

<img src="{{ site.baseurl }}/fig/playground-webui-failure.png" alt="Playground Flink WebUI" 
class="offset" width="100%" />

与此同时，数据生成器（data generator）将一直不断地往 *Input* topic中生成 `ClickEvent`s 事件。

#### Step 3: 失败恢复

一旦TaskManager重启成功，这个Job将会从失败之前最近一次成功的 
[checkpoint]({{ site.baseurl }}/internals/stream_checkpointing.html) 进行恢复。

{% highlight bash%}
docker-compose up -d taskmanager
{% endhighlight %}

一旦新的TaskManager注册到Flink Master，Job将再次处于"RUNNING"状态。
接着它将快速处理Kafka input事件的全部积压（在Job中断期间累积的数据），
并以更高的速度(> 24条记录/分钟)产生输出，直到它追上kafka的lag延迟。
观察 *output* topic输出，
你会看到在每一个时间窗口中都有按 `page`s 进行分组的记录，而且计数刚好是1000。
由于我们使用的是 [FlinkKafkaProducer]({site.base url}}/dev/connectors/kafka.html#kafka-producers-and-fault-tolerance) "at-least-once"模式，因此你可能会看到一些记录输出两次。

### Job升级与扩容

升级Flink作业一般都需要两步：第一，使用 [Savepoint]({{site.base_url}}/ops/state/savepoints.html) 优雅地停止Flink Job。
Savepoint是整个应用程序状态的一次快照（类似于checkpoint），该快照是在一个明确定义的、全局一致的时间点生成的。第二，从Savepoint恢复启动待升级的Flink Job。
在此，“升级”包含如下几种含义：

* 配置升级（比如Job并行度修改）
* Job拓扑升级（比如添加或者删除算子）
* Job的用户自定义函数升级

在开始升级之前，你可能需要实时查看 *Output* topic输出，
以便观察在升级过程中没有数据丢失或损坏。

{% highlight bash%}
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
{% endhighlight %}

#### Step 1: 停止Job

要优雅停止Job，需要使用JobID通过CLI或REST API调用“stop”命令。
JobID可以通过[获取所有运行中的Job](#获取所有运行中的Job)接口或Flink UI界面获取，拿到JobID后就可以继续停止作业了：

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**命令**
{% highlight bash %}
docker-compose run --no-deps client flink stop <job-id>
{% endhighlight %}
**预期输出**
{% highlight bash %}
Suspending job "<job-id>" with a savepoint.
Suspended job "<job-id>" with a savepoint.
{% endhighlight %}
</div>
 <div data-lang="REST API" markdown="1">
 
 **请求**
{% highlight bash %}
# 停止Job
curl -X POST localhost:8081/jobs/<job-id>/stop -d '{"drain": false}'
{% endhighlight %}

**预期响应 (结果已格式化)**
{% highlight json %}
{
  "request-id": "<trigger-id>"
}
{% endhighlight %}

**请求**
{% highlight bash %}
# 查看停止结果
 curl localhost:8081/jobs/<job-id>/savepoints/<trigger-id>
{% endhighlight %}

**预期响应 (结果已格式化)**
{% highlight json %}
{
  "status": {
    "id": "COMPLETED"
  },
  "operation": {
    "location": "<savepoint-path>"
  }

{% endhighlight %}
</div>
</div>

Savepoint已保存在`state.savepoint.dir`指定的路径中，该配置在 *flink-conf.yaml* 
中定义， *flink-conf.yaml* 挂载在本机的 */tmp/flink-savepoints-directory/* 目录下。
在下一步操作中我们会用到这个Savepoint路径，如果我们是通过REST API操作的，
那么Savepoint路径会随着响应结果一起返回，我们可以直接查看文件系统来确认Savepoint保存情况。

**命令**
{% highlight bash %}
ls -lia /tmp/flink-savepoints-directory
{% endhighlight %}

**预期输出**
{% highlight bash %}
total 0
  17 drwxr-xr-x   3 root root   60 17 jul 17:05 .
   2 drwxrwxrwt 135 root root 3420 17 jul 17:09 ..
1002 drwxr-xr-x   2 root root  140 17 jul 17:05 savepoint-<short-job-id>-<uuid>
{% endhighlight %}

#### Step 2a: 重启Job(不作任何变更)

现在你可以从这个Savepoint重新启动升级的Job，为了简单起见，不对该Job作任何变更就直接重启。

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**命令**
{% highlight bash %}
docker-compose run --no-deps client flink run -s <savepoint-path> -d /opt/flink/examples/streaming/ClickEventCount.jar --bootstrap.servers kafka:9092 --checkpointing --event-time
{% endhighlight %}
**预期输出**
{% highlight bash %}
Starting execution of program
Job has been submitted with JobID <job-id>
{% endhighlight %}
</div>
<div data-lang="REST API" markdown="1">

**请求**
{% highlight bash %}
# 上传JAR
curl -X POST -H "Expect:" -F "jarfile=@/opt/flink/examples/streaming/ClickEventCount.jar" http://localhost:8081/jars/upload
{% endhighlight %}

**预期响应 (结果已格式化)**
{% highlight json %}
{
  "filename": "/tmp/flink-web-<uuid>/flink-web-upload/<jar-id>",
  "status": "success"
}

{% endhighlight %}

**请求**
{% highlight bash %}
# 提交Job
curl -X POST http://localhost:8081/jars/<jar-id>/run -d {"programArgs": "--bootstrap.servers kafka:9092 --checkpointing --event-time", "savepointPath": "<savepoint-path>"}
{% endhighlight %}
**预期响应 (结果已格式化)**
{% highlight json %}
{
  "jobid": "<job-id>"
}
{% endhighlight %}
</div>
</div>

一旦该Job再次处于"RUNNING"状态，你将从 *output* Topic中看到数据在快速输出，
因为刚启动的Job正在处理停止期间积压的大量数据。另外，你还会看到在升级期间
没有产生任何数据丢失：所有窗口都在输出1000。

#### Step 2b: 重启Job(修改并行度)

在从Savepoint重启Job之前，你还可以通过修改并行度来达到扩容Job的目的。

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**命令**
{% highlight bash %}
docker-compose run --no-deps client flink run -p 3 -s <savepoint-path> -d /opt/flink/examples/streaming/ClickEventCount.jar --bootstrap.servers kafka:9092 --checkpointing --event-time
{% endhighlight %}
**预期输出**
{% highlight bash %}
Starting execution of program
Job has been submitted with JobID <job-id>
{% endhighlight %}
</div>
<div data-lang="REST API" markdown="1">

**请求**
{% highlight bash %}
# Uploading the JAR
curl -X POST -H "Expect:" -F "jarfile=@/opt/flink/examples/streaming/ClickEventCount.jar" http://localhost:8081/jars/upload
{% endhighlight %}

**预期响应 (结果已格式化)**
{% highlight json %}
{
  "filename": "/tmp/flink-web-<uuid>/flink-web-upload/<jar-id>",
  "status": "success"
}

{% endhighlight %}

**请求**
{% highlight bash %}
# 提交Job
curl -X POST http://localhost:8081/jars/<jar-id>/run -d {"parallelism": 3, "programArgs": "--bootstrap.servers kafka:9092 --checkpointing --event-time", "savepointPath": "<savepoint-path>"}
{% endhighlight %}
**预期响应 (结果已格式化)**
{% highlight json %}
{
  "jobid": "<job-id>"
}
{% endhighlight %}
</div>
</div>
现在Job已重新提交，但由于我们提高了并行度所以导致TaskSlots不够用（1个TaskSlot可用，总共需要3个），
最终Job会重启失败。通过如下命令：
{% highlight bash %}
docker-compose scale taskmanager=2
{% endhighlight %}
你可以向Flink集群添加第二个TaskManager，它会自动向Flink Master注册，TaskManager注册完成后，Job会再次处于 "RUNNING" 状态。

一旦Job再次运行起来，从 *output* Topic的输出中你会看到在扩容期间数据依然没有丢失：
所有窗口的计数都正好是1000。

### 查询Job指标

可以通过Flink Master提供的REST API来获取系统和用户 [指标]({{ site.baseurl }}/monitoring/metrics.html)

具体请求方式取决于我们想查询哪类指标，Job相关的指标分类可通过 `jobs/<job-id>/metrics` 
获得，而要想查询某类指标的具体值则可以在请求地址后跟上 `get` 参数。

**请求**
{% highlight bash %}
curl "localhost:8081/jobs/<jod-id>/metrics?get=lastCheckpointSize"
{% endhighlight %}
**预期响应 (结果已格式化且去除了占位符)**
{% highlight json %}
[
  {
    "id": "lastCheckpointSize",
    "value": "9378"
  }
]
{% endhighlight %}

REST API不仅可以用于查询指标，还可以用于获取正在运行中的Job详细信息。

**请求**
{% highlight bash %}
# find the vertex-id of the vertex of interest
curl localhost:8081/jobs/<jod-id>
{% endhighlight %}

**预期响应 (结果已格式化)**
{% highlight json %}
{
  "jid": "<job-id>",
  "name": "Click Event Count",
  "isStoppable": false,
  "state": "RUNNING",
  "start-time": 1564467066026,
  "end-time": -1,
  "duration": 374793,
  "now": 1564467440819,
  "timestamps": {
    "CREATED": 1564467066026,
    "FINISHED": 0,
    "SUSPENDED": 0,
    "FAILING": 0,
    "CANCELLING": 0,
    "CANCELED": 0,
    "RECONCILING": 0,
    "RUNNING": 1564467066126,
    "FAILED": 0,
    "RESTARTING": 0
  },
  "vertices": [
    {
      "id": "<vertex-id>",
      "name": "ClickEvent Source",
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1564467066423,
      "end-time": -1,
      "duration": 374396,
      "tasks": {
        "CREATED": 0,
        "FINISHED": 0,
        "DEPLOYING": 0,
        "RUNNING": 2,
        "CANCELING": 0,
        "FAILED": 0,
        "CANCELED": 0,
        "RECONCILING": 0,
        "SCHEDULED": 0
      },
      "metrics": {
        "read-bytes": 0,
        "read-bytes-complete": true,
        "write-bytes": 5033461,
        "write-bytes-complete": true,
        "read-records": 0,
        "read-records-complete": true,
        "write-records": 166351,
        "write-records-complete": true
      }
    },
    {
      "id": "<vertex-id>",
      "name": "Timestamps/Watermarks",
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1564467066441,
      "end-time": -1,
      "duration": 374378,
      "tasks": {
        "CREATED": 0,
        "FINISHED": 0,
        "DEPLOYING": 0,
        "RUNNING": 2,
        "CANCELING": 0,
        "FAILED": 0,
        "CANCELED": 0,
        "RECONCILING": 0,
        "SCHEDULED": 0
      },
      "metrics": {
        "read-bytes": 5066280,
        "read-bytes-complete": true,
        "write-bytes": 5033496,
        "write-bytes-complete": true,
        "read-records": 166349,
        "read-records-complete": true,
        "write-records": 166349,
        "write-records-complete": true
      }
    },
    {
      "id": "<vertex-id>",
      "name": "ClickEvent Counter",
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1564467066469,
      "end-time": -1,
      "duration": 374350,
      "tasks": {
        "CREATED": 0,
        "FINISHED": 0,
        "DEPLOYING": 0,
        "RUNNING": 2,
        "CANCELING": 0,
        "FAILED": 0,
        "CANCELED": 0,
        "RECONCILING": 0,
        "SCHEDULED": 0
      },
      "metrics": {
        "read-bytes": 5085332,
        "read-bytes-complete": true,
        "write-bytes": 316,
        "write-bytes-complete": true,
        "read-records": 166305,
        "read-records-complete": true,
        "write-records": 6,
        "write-records-complete": true
      }
    },
    {
      "id": "<vertex-id>",
      "name": "ClickEventStatistics Sink",
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1564467066476,
      "end-time": -1,
      "duration": 374343,
      "tasks": {
        "CREATED": 0,
        "FINISHED": 0,
        "DEPLOYING": 0,
        "RUNNING": 2,
        "CANCELING": 0,
        "FAILED": 0,
        "CANCELED": 0,
        "RECONCILING": 0,
        "SCHEDULED": 0
      },
      "metrics": {
        "read-bytes": 20668,
        "read-bytes-complete": true,
        "write-bytes": 0,
        "write-bytes-complete": true,
        "read-records": 6,
        "read-records-complete": true,
        "write-records": 0,
        "write-records-complete": true
      }
    }
  ],
  "status-counts": {
    "CREATED": 0,
    "FINISHED": 0,
    "DEPLOYING": 0,
    "RUNNING": 4,
    "CANCELING": 0,
    "FAILED": 0,
    "CANCELED": 0,
    "RECONCILING": 0,
    "SCHEDULED": 0
  },
  "plan": {
    "jid": "<job-id>",
    "name": "Click Event Count",
    "nodes": [
      {
        "id": "<vertex-id>",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "ClickEventStatistics Sink",
        "inputs": [
          {
            "num": 0,
            "id": "<vertex-id>",
            "ship_strategy": "FORWARD",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "<vertex-id>",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "ClickEvent Counter",
        "inputs": [
          {
            "num": 0,
            "id": "<vertex-id>",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "<vertex-id>",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "Timestamps/Watermarks",
        "inputs": [
          {
            "num": 0,
            "id": "<vertex-id>",
            "ship_strategy": "FORWARD",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "<vertex-id>",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "ClickEvent Source",
        "optimizer_properties": {}
      }
    ]
  }
}
{% endhighlight %}

请查阅 [REST API 参考](https：/ci.apache.org/Projects/Flink/Flink-docs-Release-1.8/监测点/REST_api.html#api)，该参考上有完整的指标查询接口信息，包括如何查询不同种类的指标（例如，TaskManager指标）。

{%  top %}

## 延伸拓展

你可能已经注意到了，*Click Event Count* 这个Job在启动时总是会带上 `--checkpointing` 和 `--event-time` 两个参数，
如果我们去除这两个参数，那么Job的行为也会随之改变。

* `--checkpointing` 参数开启了检查点 [checkpoint]({{ site.baseurl }}/internals/stream_checkpointing.html) 配置，检查点是Flink容错机制的重要保证。
如果你没有开启检查点，那么在 
[Job失败与恢复](#job失败与恢复) 这一节中，你将会看到数据丢失现象发生。

* `--event-time` 参数开启了Job的 [事件时间]({{ site.baseurl }}/dev/event_time.html) 机制，该机制会使用`ClickEvent`自带的时间戳进行统计。
如果不指定该参数，Flink将结合当前机器时间使用事件处理时间进行统计。如此一来，每个窗口计数将不再是准确的1000了。
