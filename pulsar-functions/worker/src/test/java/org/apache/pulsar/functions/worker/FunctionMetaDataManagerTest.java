/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.worker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FunctionMetaDataManagerTest {

    static byte[] producerByteArray;

    private static PulsarClient mockPulsarClient() throws PulsarClientException {
        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
        when(builder.topic(anyString())).thenReturn(builder);
        when(builder.producerName(anyString())).thenReturn(builder);

        Producer producer = mock(Producer.class);
        TypedMessageBuilder messageBuilder = mock(TypedMessageBuilder.class);
        when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
        doAnswer(invocation -> {
            Object arg0 = invocation.getArgument(0);
            FunctionMetaDataManagerTest.producerByteArray = (byte[])arg0;
            return messageBuilder;
        }).when(messageBuilder).value(any());
        when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
        when(producer.newMessage()).thenReturn(messageBuilder);

        when(builder.create()).thenReturn(producer);

        PulsarClient client = mock(PulsarClient.class);
        when(client.newProducer()).thenReturn(builder);

        return client;
    }

    @Test
    public void testListFunctions() throws PulsarClientException {
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(new WorkerConfig(),
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));

        Map<String, Function.FunctionMetaData> functionMetaDataMap1 = new HashMap<>();
        Function.FunctionMetaData f1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1")).build();
        functionMetaDataMap1.put("func-1", f1);
        Function.FunctionMetaData f2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-2")).build();
        functionMetaDataMap1.put("func-2", f2);
        Function.FunctionMetaData f3 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-3")).build();
        Map<String, Function.FunctionMetaData> functionMetaDataInfoMap2 = new HashMap<>();
        functionMetaDataInfoMap2.put("func-3", f3);


        functionMetaDataManager.functionMetaDataMap.put("tenant-1", new HashMap<>());
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-1", functionMetaDataMap1);
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-2", functionMetaDataInfoMap2);

        Assert.assertEquals(0, functionMetaDataManager.listFunctions(
                "tenant", "namespace").size());
        Assert.assertEquals(2, functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").size());
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").contains(f1));
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").contains(f2));
        Assert.assertEquals(1, functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-2").size());
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-2").contains(f3));
    }

    @Test
    public void testUpdateIfLeaderFunctionWithoutCompaction() throws PulsarClientException {
        testUpdateIfLeaderFunction(false);
    }

    @Test
    public void testUpdateIfLeaderFunctionWithCompaction() throws PulsarClientException {
        testUpdateIfLeaderFunction(true);
    }

    private void testUpdateIfLeaderFunction(boolean compact) throws PulsarClientException {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setUseCompactedMetadataTopic(compact);
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")).build();

        // update when you are not the leader
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            Assert.assertTrue(false);
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Not the leader");
        }

        // become leader
        functionMetaDataManager.acquireLeadership();
        // Now w should be able to really update
        functionMetaDataManager.updateFunctionOnLeader(m1, false);
        if (compact) {
            Assert.assertTrue(Arrays.equals(m1.toByteArray(), producerByteArray));
        } else {
            Assert.assertFalse(Arrays.equals(m1.toByteArray(), producerByteArray));
        }

        // outdated request
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Update request ignored because it is out of date. Please try again.");
        }
        // udpate with new version
        Function.FunctionMetaData m2 = m1.toBuilder().setVersion(2).build();
        functionMetaDataManager.updateFunctionOnLeader(m2, false);
        if (compact) {
            Assert.assertTrue(Arrays.equals(m2.toByteArray(), producerByteArray));
        } else {
            Assert.assertFalse(Arrays.equals(m2.toByteArray(), producerByteArray));
        }
    }

    @Test
<<<<<<< HEAD
    public void upsertFunction() throws PulsarClientException {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")).build();

        Mockito.doReturn(null).when(functionMetaDataManager).submit(any(Request.ServiceRequest.class));
        functionMetaDataManager.upsertFunction(m1);
        verify(functionMetaDataManager, times(1)).submit(any(Request.ServiceRequest.class));
        verify(functionMetaDataManager).submit(argThat(new ArgumentMatcher<Request.ServiceRequest>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Request.ServiceRequest) {
                    Request.ServiceRequest serviceRequest = (Request.ServiceRequest) o;
                    if (!serviceRequest.getWorkerId().equals(workerConfig.getWorkerId())) {
                        return false;
                    }
                    if (!serviceRequest.getServiceRequestType().equals(Request.ServiceRequest.ServiceRequestType
                            .UPSERT)) {
                        return false;
                    }
                    if (!serviceRequest.getFunctionMetaData().equals(m1)) {
                        return false;
                    }
                    if (serviceRequest.getFunctionMetaData().getVersion() != 0) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

        // already have record
        long version = 5;
        functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient()));
        Map<String, Function.FunctionMetaData> functionMetaDataMap = new HashMap<>();
        Function.FunctionMetaData m2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();
        functionMetaDataMap.put("func-1", m2);
        functionMetaDataManager.functionMetaDataMap.put("tenant-1", new HashMap<>());
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-1", functionMetaDataMap);
        Mockito.doReturn(null).when(functionMetaDataManager).submit(any(Request.ServiceRequest.class));

        functionMetaDataManager.upsertFunction(m2);
        verify(functionMetaDataManager, times(1)).submit(any(Request.ServiceRequest.class));
        verify(functionMetaDataManager).submit(argThat(new ArgumentMatcher<Request.ServiceRequest>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Request.ServiceRequest) {
                    Request.ServiceRequest serviceRequest = (Request.ServiceRequest) o;
                    if (!serviceRequest.getWorkerId().equals(workerConfig.getWorkerId())) return false;
                    if (!serviceRequest.getServiceRequestType().equals(
                            Request.ServiceRequest.ServiceRequestType.UPSERT)) {
                        return false;
                    }
                    if (!serviceRequest.getFunctionMetaData().getFunctionDetails().equals(m2.getFunctionDetails())) {
                        return false;
                    }
                    if (serviceRequest.getFunctionMetaData().getVersion() != (version + 1)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

    }

    @Test
    public void testStopFunction() throws PulsarClientException {

        long version = 5;
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient()));

        Map<String, Function.FunctionMetaData> functionMetaDataMap1 = new HashMap<>();
        Function.FunctionMetaData f1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder().setName("func-1").setParallelism(2)).setVersion(version).build();
        functionMetaDataMap1.put("func-1", f1);

        Assert.assertTrue(functionMetaDataManager.canChangeState(f1, 0, Function.FunctionState.STOPPED));
        Assert.assertFalse(functionMetaDataManager.canChangeState(f1, 0, Function.FunctionState.RUNNING));
        Assert.assertFalse(functionMetaDataManager.canChangeState(f1, 2, Function.FunctionState.STOPPED));
        Assert.assertFalse(functionMetaDataManager.canChangeState(f1, 2, Function.FunctionState.RUNNING));

        functionMetaDataManager.functionMetaDataMap.put("tenant-1", new HashMap<>());
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-1", functionMetaDataMap1);

        Mockito.doReturn(null).when(functionMetaDataManager).submit(any(Request.ServiceRequest.class));

        functionMetaDataManager.changeFunctionInstanceStatus("tenant-1", "namespace-1", "func-1", 0, false);

        verify(functionMetaDataManager, times(1)).submit(any(Request.ServiceRequest.class));
        verify(functionMetaDataManager).submit(argThat(new ArgumentMatcher<Request.ServiceRequest>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Request.ServiceRequest) {
                    Request.ServiceRequest serviceRequest = (Request.ServiceRequest) o;
                    if (!serviceRequest.getWorkerId().equals(workerConfig.getWorkerId())) return false;
                    if (!serviceRequest.getServiceRequestType().equals(
                            Request.ServiceRequest.ServiceRequestType.UPDATE)) {
                        return false;
                    }
                    if (!serviceRequest.getFunctionMetaData().getFunctionDetails().equals(f1.getFunctionDetails())) {
                        return false;
                    }
                    if (serviceRequest.getFunctionMetaData().getVersion() != (version + 1)) {
                        return false;
                    }
                    Map<Integer, Function.FunctionState> stateMap = serviceRequest.getFunctionMetaData().getInstanceStatesMap();
                    if (stateMap == null || stateMap.isEmpty()) {
                        return false;
                    }
                    if (stateMap.get(1) != Function.FunctionState.RUNNING) {
                        return false;
                    }
                    if (stateMap.get(0) != Function.FunctionState.STOPPED) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
=======
    public void deregisterFunctionWithoutCompaction() throws PulsarClientException {
        deregisterFunction(false);
>>>>>>> upstream/master
    }

    @Test
    public void deregisterFunctionWithCompaction() throws PulsarClientException {
        deregisterFunction(true);
    }

    private void deregisterFunction(boolean compact) throws PulsarClientException {
        SchedulerManager mockedScheduler = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setUseCompactedMetadataTopic(compact);
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mockedScheduler,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).build();

        // Try deleting when you are not the leader
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            Assert.assertTrue(false);
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Not the leader");
        }

        // become leader
        functionMetaDataManager.acquireLeadership();
        verify(mockedScheduler, times(0)).schedule();
        // Now try deleting
        functionMetaDataManager.updateFunctionOnLeader(m1, true);
        // make sure schedule was not called because function didn't exist.
        verify(mockedScheduler, times(0)).schedule();

        // insert function
        functionMetaDataManager.updateFunctionOnLeader(m1, false);
        verify(mockedScheduler, times(1)).schedule();

        // outdated request
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Delete request ignored because it is out of date. Please try again.");
        }
        verify(mockedScheduler, times(1)).schedule();

        // udpate with new version
        m1 = m1.toBuilder().setVersion(2).build();
        functionMetaDataManager.updateFunctionOnLeader(m1, true);
        verify(mockedScheduler, times(2)).schedule();
        if (compact) {
            Assert.assertTrue(Arrays.equals("".getBytes(), producerByteArray));
        } else {
            Assert.assertFalse(Arrays.equals(m1.toByteArray(), producerByteArray));
        }
    }

    @Test
    public void testProcessRequest() throws PulsarClientException, IOException {
        WorkerConfig workerConfig = new WorkerConfig();
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));

        doReturn(true).when(functionMetaDataManager).processUpdate(any(Function.FunctionMetaData.class));
        doReturn(true).when(functionMetaDataManager).proccessDeregister(any(Function.FunctionMetaData.class));

        Request.ServiceRequest serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                        Request.ServiceRequest.ServiceRequestType.UPDATE).build();
        Message msg = mock(Message.class);
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        verify(functionMetaDataManager, times(1)).processUpdate
                (any(Function.FunctionMetaData.class));
        verify(functionMetaDataManager).processUpdate(serviceRequest.getFunctionMetaData());

        serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                Request.ServiceRequest.ServiceRequestType.INITIALIZE).build();
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                Request.ServiceRequest.ServiceRequestType.DELETE).build();
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        verify(functionMetaDataManager, times(1)).proccessDeregister(
                any(Function.FunctionMetaData.class));
        verify(functionMetaDataManager).proccessDeregister(serviceRequest.getFunctionMetaData());
    }

    @Test
    public void processUpdateTest() throws PulsarClientException {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                .setNamespace("namespace-1").setTenant("tenant-1")).build();

        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // outdated request
        try {
            functionMetaDataManager.processUpdate(m1);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Update request ignored because it is out of date. Please try again.");
        }
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // udpate with new version
        m1 = m1.toBuilder().setVersion(2).build();
        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(2))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }

    @Test
    public void processUpsertTest() throws PulsarClientException {
        long version = 5;
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient()));

        // worker has no record of function
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();

        Request.ServiceRequest serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPSERT)
                .setFunctionMetaData(m1)
                .setWorkerId("worker-1")
                .build();
        functionMetaDataManager.processUpsert(serviceRequest);
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(1)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // worker has record of function

        // request is oudated
        schedulerManager = mock(SchedulerManager.class);
        functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient()));

        Function.FunctionMetaData m3 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();
        functionMetaDataManager.setFunctionMetaData(m3);
        Function.FunctionMetaData outdated = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version - 1).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPSERT)
                .setFunctionMetaData(outdated)
                .setWorkerId("worker-1")
                .build();
        functionMetaDataManager.processUpsert(serviceRequest);

        Assert.assertEquals(m3, functionMetaDataManager.getFunctionMetaData(
                "tenant-1", "namespace-1", "func-1"));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();

        Function.FunctionMetaData outdated2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPSERT)
                .setFunctionMetaData(outdated2)
                .setWorkerId("worker-2")
                .build();
        functionMetaDataManager.processUpsert(serviceRequest);
        Assert.assertEquals(m3, functionMetaDataManager.getFunctionMetaData(
                "tenant-1", "namespace-1", "func-1"));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();

        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // schedule
        schedulerManager = mock(SchedulerManager.class);
        functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient()));

        Function.FunctionMetaData m4 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();
        functionMetaDataManager.setFunctionMetaData(m4);
        Function.FunctionMetaData m5 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version + 1).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPSERT)
                .setFunctionMetaData(m5)
                .setWorkerId("worker-2")
                .build();
        functionMetaDataManager.processUpsert(serviceRequest);

        verify(functionMetaDataManager, times(2))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(1)).schedule();

        Assert.assertEquals(m1.toBuilder().setVersion(version + 1).build(),
                functionMetaDataManager.functionMetaDataMap.get(
                        "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }

    @Test
    public void processDeregister() throws PulsarClientException {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setVersion(1)
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).build();

        Assert.assertFalse(functionMetaDataManager.proccessDeregister(m1));
        verify(functionMetaDataManager, times(0))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(0, functionMetaDataManager.functionMetaDataMap.size());

        // insert something
        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // outdated delete request
        try {
            functionMetaDataManager.proccessDeregister(m1);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Delete request ignored because it is out of date. Please try again.");
        }
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // delete now
        m1 = m1.toBuilder().setVersion(2).build();
        Assert.assertTrue(functionMetaDataManager.proccessDeregister(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(0, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }
}