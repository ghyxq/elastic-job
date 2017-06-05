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

package com.dangdang.ddframe.job.lite.internal.config;

import com.dangdang.ddframe.job.exception.JobConfigurationException;
import com.dangdang.ddframe.job.exception.JobExecutionEnvironmentException;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.env.TimeService;
import com.google.common.base.Optional;

/**
 * 弹性化分布式作业配置服务.
 * 其中主要包含了一个属性：JobNodeStorage
 * @author zhangliang
 * @author caohao
 */
public final class ConfigurationService {
    
    private final TimeService timeService;
    
    private final JobNodeStorage jobNodeStorage;
    
    public ConfigurationService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        timeService = new TimeService();
    }
    
    /**
     * 读取作业配置.
     * 
     * @param fromCache 是否从缓存中读取
     * @return 作业配置
     */
    public LiteJobConfiguration load(final boolean fromCache) {
        String result;
        if (fromCache) {
            result = jobNodeStorage.getJobNodeData(ConfigurationNode.ROOT);
            if (null == result) {
                result = jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
            }
        } else {
            result = jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT);
        }
        return LiteJobConfigurationGsonFactory.fromJson(result);
    }
    
    /**
     * 持久化分布式作业配置信息.
     * 
     * @param liteJobConfig 作业配置
     */
    public void persist(final LiteJobConfiguration liteJobConfig) {
    	//其中调用接口在zk上创建目录，
         checkConflictJob(liteJobConfig);
         
         //如果作业实现类名称一致，则可以将相同的配置进行更新.
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT) || liteJobConfig.isOverwrite()) {
           //这里会更新路径信息，如果没有改路径则创建一个，如果有的话则直接更新内容，主要的config中的配置信息就是在这里第一更新的
        	jobNodeStorage.replaceJobNode(ConfigurationNode.ROOT, LiteJobConfigurationGsonFactory.toJson(liteJobConfig));
        }
    }
    
    /**
     * 检查传入的配置和zk上的节点配置是否相同，主要检查jobclass是否一致.
     * 
     * @param liteJobConfig 作业配置
     */
    private void checkConflictJob(final LiteJobConfiguration liteJobConfig) {
    	//其中会调用接口在zk上创建目录
        Optional<LiteJobConfiguration> liteJobConfigFromZk = find();
        
        //如果liteJobConfigFromZk非空，并且liteJobConfigFromZk中的jobclass和传入的配置不相同的话则抛异常
        if (liteJobConfigFromZk.isPresent() && !liteJobConfigFromZk.get().getTypeConfig().getJobClass().equals(liteJobConfig.getTypeConfig().getJobClass())) {
            throw new JobConfigurationException("Job conflict with register center. The job '%s' in register center's class is '%s', your job class is '%s'", 
                    liteJobConfig.getJobName(), liteJobConfigFromZk.get().getTypeConfig().getJobClass(), liteJobConfig.getTypeConfig().getJobClass());
        }
    }
    
    /**
     * 从zk上的config节点查询是否存在config节点，如果存在则返回配置信息Optional<LiteJobConfiguration>
     * 
     */
    private Optional<LiteJobConfiguration> find() {
    	//isJobNodeExisted会在zk上创建接口
        if (!jobNodeStorage.isJobNodeExisted(ConfigurationNode.ROOT)) {
            return Optional.absent();
        }
        LiteJobConfiguration result = LiteJobConfigurationGsonFactory.fromJson(jobNodeStorage.getJobNodeDataDirectly(ConfigurationNode.ROOT));
        if (null == result) {
            // TODO 应该删除整个job node,并非仅仅删除config node
            jobNodeStorage.removeJobNodeIfExisted(ConfigurationNode.ROOT);
        }
        return Optional.fromNullable(result);
    }
    
    /**
     * 检查本机与注册中心的时间误差秒数是否在允许范围.
     * 
     * @throws JobExecutionEnvironmentException 本机与注册中心的时间误差秒数不在允许范围所抛出的异常
     */
    public void checkMaxTimeDiffSecondsTolerable() throws JobExecutionEnvironmentException {
        int maxTimeDiffSeconds =  load(true).getMaxTimeDiffSeconds();
        if (-1  == maxTimeDiffSeconds) {
            return;
        }
        long timeDiff = Math.abs(timeService.getCurrentMillis() - jobNodeStorage.getRegistryCenterTime());
        if (timeDiff > maxTimeDiffSeconds * 1000L) {
            throw new JobExecutionEnvironmentException(
                    "Time different between job server and register center exceed '%s' seconds, max time different is '%s' seconds.", timeDiff / 1000, maxTimeDiffSeconds);
        }
    }
}
