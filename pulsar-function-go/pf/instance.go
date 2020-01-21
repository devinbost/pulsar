//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pf

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"math"
	"strconv"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	pb "github.com/apache/pulsar/pulsar-function-go/pb"
)

type goInstance struct {
	function  function
	context   *FunctionContext
	producer  pulsar.Producer
	consumers map[string]pulsar.Consumer
	client    pulsar.Client
	lastHealthCheckTs int64
	stats statWithLabelValues
}

func (gi *goInstance) getMetricsLabels() []string{
	//metrics_labels := []string{"test-tenant","test-tenant/test-namespace", "test-name", "1234", "test-cluster",
	//	"test-tenant/test-namespace/test-name"}
	metrics_labels := []string{
		gi.context.GetFuncTenant(),
		gi.context.GetTenantAndNamespace(),
		gi.context.GetFuncName(),
		gi.context.GetFuncID(),
		gi.context.GetClusterName(),
		gi.context.GetTenantAndNamespaceAndName(),
	}
	return metrics_labels
}
// newGoInstance init goInstance and init function context
func newGoInstance() *goInstance {
	goInstance := &goInstance{
		context:   NewFuncContext(),
		consumers: make(map[string]pulsar.Consumer),
	}
	now := time.Now()
	goInstance.lastHealthCheckTs = now.UnixNano()
	goInstance.stats = NewStatWithLabelValues(goInstance.getMetricsLabels()...)
	return goInstance
}

func (gi *goInstance) processSpawnerHealthCheckTimer(tkr *time.Ticker){
	log.Info("Starting processSpawnerHealthCheckTimer")
	now := time.Now()
	maxIdleTime := gi.context.GetMaxIdleTime()
	timeSinceLastCheck := now.UnixNano() - gi.lastHealthCheckTs
	if (timeSinceLastCheck) > (maxIdleTime)  {
		log.Errorf("Haven't received health check from spawner in a while. Stopping instance...")
		gi.close()
		// os.Exit(1)
		tkr.Stop()
	}
}

func (gi *goInstance) startScheduler(){
	if gi.context.instanceConf.expectedHealthCheckInterval > 0 {
		log.Info("Starting Scheduler")
		go func() {
			log.Info("Started Scheduler")
			tkr := time.NewTicker(time.Millisecond * 1000 * gi.context.GetExpectedHealthCheckIntervalAsDuration())
			for range tkr.C {
				log.Info("Starting Timer")
				go gi.processSpawnerHealthCheckTimer(tkr)
			}
		}()
	}
}

func (gi *goInstance) startFunction(function function) error {
	gi.function = function

	// start proccess spawner health check timer
	now := time.Now();
	gi.lastHealthCheckTs = now.UnixNano()

	gi.startScheduler()

	err := gi.setupClient()
	if err != nil {
		log.Errorf("setup client failed, error is:%v", err)
		return err
	}
	err = gi.setupProducer()
	if err != nil {
		log.Errorf("setup producer failed, error is:%v", err)
		return err
	}
	channel, err := gi.setupConsumer()
	if err != nil {
		log.Errorf("setup consumer failed, error is:%v", err)
		return err
	}
	err = gi.setupLogHandler()
	if err != nil {
		log.Errorf("setup log appender failed, error is:%v", err)
		return err
	}

	idleDuration := getIdleTimeout(time.Millisecond * gi.context.instanceConf.killAfterIdleMs)
	idleTimer := time.NewTimer(idleDuration)
	defer idleTimer.Stop()

	servicer := InstanceControlServicer{goInstance:gi}
	servicer.serve(gi)

CLOSE:
	for {
		idleTimer.Reset(idleDuration)
		select {
		case cm := <-channel:
			msgInput := cm.Message
			atMostOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATMOST_ONCE
			atLeastOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE
			autoAck := gi.context.instanceConf.funcDetails.AutoAck
			if autoAck && atMostOnce {
				gi.ackInputMessage(msgInput)
			}

			gi.stats.incr_total_received()
			gi.addLogTopicHandler()

			gi.stats.set_last_invocation()
			gi.stats.process_time_start()

			output, err := gi.handlerMsg(msgInput)
			if err != nil {
				log.Errorf("handler message error:%v", err)
				if autoAck && atLeastOnce {
					gi.nackInputMessage(msgInput)
				}
				gi.stats.incr_total_user_exceptions(err)
				return err
			}

			gi.processResult(msgInput, output)

			gi.stats.process_time_end() // Should this be called here or before processResult(..)?
			gi.stats.incr_total_processed_successfully()

		case <-idleTimer.C:
			close(channel)
			break CLOSE
		}
	}

	gi.closeLogTopic()
	gi.close()
	return nil
}

func (gi *goInstance) setupClient() error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{

		URL: gi.context.instanceConf.pulsarServiceURL,
	})
	if err != nil {
		log.Errorf("create client error:%v", err)
		gi.stats.incr_total_sys_exceptions(err)
		return err
	}
	gi.client = client
	return nil
}

func (gi *goInstance) setupProducer() (err error) {
	if gi.context.instanceConf.funcDetails.Sink.Topic != "" && len(gi.context.instanceConf.funcDetails.Sink.Topic) > 0 {
		log.Debugf("Setting up producer for topic %s", gi.context.instanceConf.funcDetails.Sink.Topic)
		properties := getProperties(getDefaultSubscriptionName(
			gi.context.instanceConf.funcDetails.Tenant,
			gi.context.instanceConf.funcDetails.Namespace,
			gi.context.instanceConf.funcDetails.Name), gi.context.instanceConf.instanceID)
		gi.producer, err = gi.client.CreateProducer(pulsar.ProducerOptions{
			Topic:                   gi.context.instanceConf.funcDetails.Sink.Topic,
			Properties:              properties,
			CompressionType:         pulsar.LZ4,
			BlockIfQueueFull:        true,
			Batching:                true,
			BatchingMaxPublishDelay: time.Millisecond * 10,
			// set send timeout to be infinity to prevent potential deadlock with consumer
			// that might happen when consumer is blocked due to unacked messages
			SendTimeout: 0,
		})
		if err != nil {
			gi.stats.incr_total_sys_exceptions(err)
			log.Errorf("create producer error:%s", err.Error())
			return err
		}
	}
	return nil
}

func (gi *goInstance) setupConsumer() (chan pulsar.ConsumerMessage, error) {
	subscriptionType := pulsar.Shared
	if int32(gi.context.instanceConf.funcDetails.Source.SubscriptionType) == pb.SubscriptionType_value["FAILOVER"] {
		subscriptionType = pulsar.Failover
	}

	funcDetails := gi.context.instanceConf.funcDetails
	subscriptionName := funcDetails.Tenant + "/" + funcDetails.Namespace + "/" + funcDetails.Name

	properties := getProperties(getDefaultSubscriptionName(
		funcDetails.Tenant,
		funcDetails.Namespace,
		funcDetails.Name), gi.context.instanceConf.instanceID)

	channel := make(chan pulsar.ConsumerMessage)

	var (
		consumer pulsar.Consumer
		err      error
	)

	for topic, consumerConf := range funcDetails.Source.InputSpecs {
		log.Debugf("Setting up consumer for topic: %s with subscription name: %s", topic, subscriptionName)
		if consumerConf.ReceiverQueueSize != nil {
			if consumerConf.IsRegexPattern {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:     topic,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              subscriptionType,
					MessageChannel:    channel,
				})
			} else {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:             topic,
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              subscriptionType,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					MessageChannel:    channel,
				})
			}
		} else {
			if consumerConf.IsRegexPattern {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:    topic,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             subscriptionType,
					MessageChannel:   channel,
				})
			} else {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:            topic,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             subscriptionType,
					MessageChannel:   channel,
				})

			}
		}

		if err != nil {
			log.Errorf("create consumer error:%s", err.Error())
			gi.stats.incr_total_sys_exceptions(err)
			return nil, err
		}
		gi.consumers[topic] = consumer
	}
	return channel, nil
}

func (gi *goInstance) handlerMsg(input pulsar.Message) (output []byte, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = NewContext(ctx, gi.context)
	msgInput := input.Payload()
	return gi.function.process(ctx, msgInput)
}

func (gi *goInstance) processResult(msgInput pulsar.Message, output []byte) {
	atLeastOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE
	atMostOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATMOST_ONCE
	autoAck := gi.context.instanceConf.funcDetails.AutoAck

	if output != nil && gi.context.instanceConf.funcDetails.Sink.Topic != "" {
		asyncMsg := pulsar.ProducerMessage{
			Payload: output,
		}
		// Attempt to send the message asynchronously and handle the response
		gi.producer.SendAsync(context.Background(), asyncMsg, func(message pulsar.ProducerMessage, e error) {
			if e != nil {
				if autoAck && atLeastOnce {
					gi.nackInputMessage(msgInput)
				}
				gi.stats.incr_total_sys_exceptions(e)
				log.Fatal(e)
			} else if autoAck && !atMostOnce {
				gi.ackInputMessage(msgInput)
				gi.stats.incr_total_processed_successfully()
			} else {
				gi.stats.incr_total_processed_successfully()
			}
		})
	} else {
		if autoAck && atLeastOnce {
			gi.ackInputMessage(msgInput)
			// Report that we processed successfully even though it's not going to an output topic?
			// We probably shouldn't...
			// gi.stats.incr_total_processed_successfully()
		}
	}
}

// ackInputMessage doesn't produce any result or the user doesn't want the result.
func (gi *goInstance) ackInputMessage(inputMessage pulsar.Message) {
	gi.consumers[inputMessage.Topic()].Ack(inputMessage)
}

func (gi *goInstance) nackInputMessage(inputMessage pulsar.Message) {
	gi.consumers[inputMessage.Topic()].Nack(inputMessage)
}

func getIdleTimeout(timeoutMilliSecond time.Duration) time.Duration {
	if timeoutMilliSecond <= 0 {
		return time.Duration(math.MaxInt64)
	}
	return timeoutMilliSecond
}

func (gi *goInstance) setupLogHandler() error {
	if gi.context.instanceConf.funcDetails.GetLogTopic() != "" {
		gi.context.logAppender = NewLogAppender(
			gi.client, //pulsar client
			gi.context.instanceConf.funcDetails.GetLogTopic(), //log topic
			getDefaultSubscriptionName(gi.context.instanceConf.funcDetails.Tenant, //fqn
				gi.context.instanceConf.funcDetails.Namespace,
				gi.context.instanceConf.funcDetails.Name),
		)
		return gi.context.logAppender.Start()
	}
	return nil
}

func (gi *goInstance) addLogTopicHandler() {
	// Clear StrEntry regardless gi.context.logAppender is set or not
	defer func() {
		log.StrEntry = nil
	}()

	if gi.context.logAppender == nil {
		log.Error("the logAppender is nil, if you want to use it, please specify `--log-topic` at startup.")
		return
	}

	for _, logByte := range log.StrEntry {
		gi.context.logAppender.Append([]byte(logByte))
	}
}

func (gi *goInstance) closeLogTopic() {
	log.Info("closing log topic...")
	if gi.context.logAppender == nil {
		return
	}
	gi.context.logAppender.Stop()
	gi.context.logAppender = nil
}

func (gi *goInstance) close() {
	log.Info("closing go instance...")
	if gi.producer != nil {
		gi.producer.Close()
	}
	if gi.consumers != nil {
		for _, consumer := range gi.consumers {
			consumer.Close()
		}
	}
	if gi.client != nil {
		gi.client.Close()
	}
}


func (gi *goInstance) healthCheck() *pb.HealthCheckResult {
	now := time.Now()
	gi.lastHealthCheckTs = now.UnixNano()
	healthCheckResult := pb.HealthCheckResult{Success: true}
	return &healthCheckResult
}

func (gi *goInstance) getFunctionStatus() *pb.FunctionStatus {
	status := pb.FunctionStatus{}
	status.Running = true
	total_received := gi.get_total_received()
	total_processed_successfully := gi.get_total_processed_successfully()
	total_user_exceptions := gi.get_total_user_exceptions()
	total_sys_exceptions := gi.get_total_sys_exceptions()
	avg_process_latency_ms := gi.get_avg_process_latency()
	last_invocation := gi.get_last_invocation()

	status.NumReceived = int64(total_received)
	status.NumSuccessfullyProcessed = int64(total_processed_successfully)
	status.NumUserExceptions = int64(total_user_exceptions)
	status.InstanceId = strconv.Itoa(gi.context.instanceConf.instanceID)

	status.NumUserExceptions = int64(total_user_exceptions)
	for _, exPair := range gi.stats.latest_user_exception{
		toAdd := pb.FunctionStatus_ExceptionInformation{}
		toAdd.ExceptionString = exPair.exception.Error()
		toAdd.MsSinceEpoch = exPair.timestamp
		status.LatestUserExceptions = append(status.LatestUserExceptions, &toAdd)
	}

	status.NumSystemExceptions = int64(total_sys_exceptions)
	for _, exPair := range gi.stats.latest_sys_exception{
		toAdd := pb.FunctionStatus_ExceptionInformation{}
		toAdd.ExceptionString = exPair.exception.Error()
		toAdd.MsSinceEpoch = exPair.timestamp
		status.LatestSystemExceptions = append(status.LatestSystemExceptions, &toAdd)
	}
	status.AverageLatency = float64(avg_process_latency_ms)
	status.LastInvocationTime = int64(last_invocation)
	return &status
}
func (gi *goInstance) getMetrics() *pb.MetricsData {

	total_received :=  gi.get_total_received()
	    total_processed_successfully := gi.get_total_processed_successfully()
	    total_user_exceptions := gi.get_total_user_exceptions()
	    total_sys_exceptions := gi.get_total_sys_exceptions()
	    avg_process_latency_ms := gi.get_avg_process_latency()
	    last_invocation := gi.get_last_invocation()

	    total_received_1min := gi.get_total_received_1min()
	    total_processed_successfully_1min := gi.get_total_processed_successfully_1min()
	    total_user_exceptions_1min := gi.get_total_user_exceptions_1min()
	    total_sys_exceptions_1min := gi.get_total_sys_exceptions_1min()
	    //avg_process_latency_ms_1min := gi.get_avg_process_latency_1min()

	    metrics_data := pb.MetricsData{}
	    // total metrics
	    metrics_data.ReceivedTotal = int64(total_received)
	    metrics_data.ProcessedSuccessfullyTotal = int64(total_processed_successfully)
	    metrics_data.SystemExceptionsTotal = int64(total_sys_exceptions)
	    metrics_data.UserExceptionsTotal = int64(total_user_exceptions)
	    metrics_data.AvgProcessLatency = float64(avg_process_latency_ms)
	    metrics_data.LastInvocation = int64(last_invocation)
	    // 1min metrics
	    metrics_data.ReceivedTotal_1Min = int64(total_received_1min)
	    metrics_data.ProcessedSuccessfullyTotal_1Min = int64(total_processed_successfully_1min)
	    metrics_data.SystemExceptionsTotal_1Min = int64(total_sys_exceptions_1min)
	    metrics_data.UserExceptionsTotal_1Min = int64(total_user_exceptions_1min)
	    //metrics_data.AvgProcessLatency_1Min = avg_process_latency_ms_1min

	    // get any user metrics
	    // Not sure yet where these are stored.
	    /*
	    user_metrics := self.contextimpl.get_metrics()
	    for metric_name, value in user_metrics.items():
	      metrics_data.userMetrics[metric_name] = value
	    */

	    return &metrics_data

	return nil
}
func (gi *goInstance) getAndResetMetrics() *pb.MetricsData {
	metricsData := gi.getMetrics()
	gi.resetMetrics()
	return metricsData
}

func (gi *goInstance) resetMetrics() *empty.Empty {
	gi.stats.reset()
 return nil
}

// This method is used to get the required metrics for Prometheus.
// Note that this doesn't distinguish between parallel function instances!
func (gi *goInstance) getMatchingMetricFunc() func(lbl *io_prometheus_client.LabelPair) bool {
	matchMetricFunc := func(lbl *io_prometheus_client.LabelPair) bool {
		return *lbl.Name == "fqfn" && *lbl.Value == gi.context.GetTenantAndNamespaceAndName()
	}
	return matchMetricFunc
}
// e.g. metricName = "pulsar_function_process_latency_ms"
func (gi *goInstance) getMatchingMetricFromRegistry(metricName string) io_prometheus_client.Metric{
	metricFamilies, err := reg.Gather()
	if err != nil {
		// Handle this.
	}
	matchFamilyFunc := func(vect *io_prometheus_client.MetricFamily) bool {
		return *vect.Name == metricName
	}
	fiteredMetricFamilies := filter(metricFamilies, matchFamilyFunc)
	if len(fiteredMetricFamilies) > 1 {
		// handle this.
		log.Error("Too many metric families for metricName = " + metricName)
		// Should we panic here instead of report an error since it reflects a code problem, not a user problem?
	}
	metricFunc := gi.getMatchingMetricFunc()
	matchingMetric := getFirstMatch(fiteredMetricFamilies[0].Metric, metricFunc)
	return *matchingMetric
}

func  (gi *goInstance) get_total_received() float32 {
	// "pulsar_function_" + "received_total", NewGaugeVec.
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_RECEIVED)
	val := metric.GetGauge().Value
	return float32(*val)
}
func (gi *goInstance) get_total_processed_successfully() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SUCCESSFULLY_PROCESSED)
	// "pulsar_function_" + "processed_successfully_total", NewGaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}
func (gi *goInstance) get_total_sys_exceptions() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SYSTEM_EXCEPTIONS)
	// "pulsar_function_"+ "system_exceptions_total", NewGaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) get_total_user_exceptions() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_USER_EXCEPTIONS)
	// "pulsar_function_" + "user_exceptions_total", NewGaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) get_avg_process_latency() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS)
	// "pulsar_function_" + "process_latency_ms", SummaryVec.
	count := metric.GetSummary().SampleCount
	sum := metric.GetSummary().SampleSum
	if *count <= 0.0 {
		return 0.0
	} else {
		return float32(*sum) / float32(*count)
	}
}

func (gi *goInstance) get_last_invocation() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + LAST_INVOCATION)
	// "pulsar_function_" + "last_invocation", GaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) get_total_processed_successfully_1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SUCCESSFULLY_PROCESSED_1min)
	// "pulsar_function_" + "processed_successfully_total_1min", GaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) get_total_sys_exceptions_1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_SYSTEM_EXCEPTIONS_1min)
	// "pulsar_function_" + "system_exceptions_total_1min", GaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) get_total_user_exceptions_1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_USER_EXCEPTIONS_1min)
	// "pulsar_function_" + "user_exceptions_total_1min", GaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}
/*
func (gi *goInstance) get_avg_process_latency_1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + PROCESS_LATENCY_MS_1min)
	// "pulsar_function_" + "process_latency_ms_1min", SummaryVec
	count := metric.GetSummary().SampleCount
	sum := metric.GetSummary().SampleSum
	if *count <= 0.0 {
		return 0.0
	} else {
		return float32(*sum) / float32(*count)
	}
}*/

func (gi *goInstance) get_total_received_1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PULSAR_FUNCTION_METRICS_PREFIX + TOTAL_RECEIVED_1min)
	// "pulsar_function_" +  "received_total_1min", GaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}