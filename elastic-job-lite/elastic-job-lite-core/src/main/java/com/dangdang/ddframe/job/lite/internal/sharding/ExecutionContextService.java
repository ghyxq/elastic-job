/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.internal.sharding;

import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.config.ShardingItemParameters;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作业运行时上下文服务.
 * 其中主要的方法就是更新或者创建ShardingContexts，ShardingContexts其中主要记录了taskid，jobname,分片信息
 * @author zhangliang
 */
public final class ExecutionContextService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ConfigurationService configService;
    
    /**
     * 初始化主要的属性
     * 
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    public ExecutionContextService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
    }
    
    /**
     * 获取当前作业服务器分片上下文.
     * 生成ShardingContexts
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    public ShardingContexts getJobShardingContext(final List<Integer> shardingItems) {
        LiteJobConfiguration liteJobConfig = configService.load(false);
        
        //更新shardingItems，没有执行的则从shardingItems中移除掉，如果最后shardingItems为空的话，则新建一个
        removeRunningIfMonitorExecution(liteJobConfig.isMonitorExecution(), shardingItems);
        if (shardingItems.isEmpty()) {
            return new ShardingContexts(buildTaskId(liteJobConfig, shardingItems), liteJobConfig.getJobName(), liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(), 
                    liteJobConfig.getTypeConfig().getCoreConfig().getJobParameter(), Collections.<Integer, String>emptyMap());
        }
        Map<Integer, String> shardingItemParameterMap = new ShardingItemParameters(liteJobConfig.getTypeConfig().getCoreConfig().getShardingItemParameters()).getMap();
        
        return new ShardingContexts(buildTaskId(liteJobConfig, shardingItems), liteJobConfig.getJobName(), liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(), 
                liteJobConfig.getTypeConfig().getCoreConfig().getJobParameter(), getAssignedShardingItemParameterMap(shardingItems, shardingItemParameterMap));
    }
    
    private String buildTaskId(final LiteJobConfiguration liteJobConfig, final List<Integer> shardingItems) {
        JobInstance jobInstance = JobRegistry.getInstance().getJobInstance(jobName);
        return Joiner.on("@-@").join(liteJobConfig.getJobName(), Joiner.on(",").join(shardingItems), "READY", 
                null == jobInstance.getJobInstanceId() ? "127.0.0.1@-@1" : jobInstance.getJobInstanceId()); 
    }
    
    /**
     * 更新final List <Integer> shardingItems，判断shardingItems中的分片是否都在运行，如果没有的话则移除
     * //sharding/%s/running"
     * 
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    private void removeRunningIfMonitorExecution(final boolean monitorExecution, final List<Integer> shardingItems) {
        if (!monitorExecution) {
            return;
        }
        List<Integer> runningShardingItems = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (isRunning(each)) {
                runningShardingItems.add(each);
            }
        }
        shardingItems.removeAll(runningShardingItems);
    }
    
    
    /**
     * 判断节点下的分片是否存在，如果存在的话说明这个分片节点正在执行
     * //sharding/%s/running"
     * 
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    private boolean isRunning(final int shardingItem) {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.getRunningNode(shardingItem));
    }
    
    /**
     * 相当于复制一份shardingItemParameterMap
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    private Map<Integer, String> getAssignedShardingItemParameterMap(final List<Integer> shardingItems, final Map<Integer, String> shardingItemParameterMap) {
        Map<Integer, String> result = new HashMap<>(shardingItemParameterMap.size(), 1);
        for (int each : shardingItems) {
            result.put(each, shardingItemParameterMap.get(each));
        }
        return result;
    }
}
