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

package com.dangdang.ddframe.job.lite.internal.election;

import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerNode;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.server.ServerStatus;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 主节点选举监听管理器.
 * 
 * @author zhangliang
 */
public final class ElectionListenerManager extends AbstractListenerManager {
    
    private final String jobName;
    
    private final LeaderNode leaderNode;
    
    private final ServerNode serverNode;
    
    private final LeaderService leaderService;
    
    private final ServerService serverService;
    
    public ElectionListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        leaderNode = new LeaderNode(jobName);
        serverNode = new ServerNode(jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    /**
     * 启动即直接在相应的节点上添加监听器，利用treecache和对应的TreeCacheListener
     * 
     * @author zhangliang
     */
    @Override
    public void start() {
        addDataListener(new LeaderElectionJobListener());
        addDataListener(new LeaderAbdicationJobListener());
    }
    
    class LeaderElectionJobListener extends AbstractJobListener {
        
    	 /**
         * 当节点变化的时候他其实会调用TreeCacheListener（这是一个接口，定义了回调的方法childEvent） 的childEvent方法，但是由于AbstractJobListener
         * 继承了TreeCacheListener，并在childEvent调用了dataChanged方法，所以后面继承的类直接重写datachanged就可以了<br>
         * 
         * 如果当前job没有shutdown，并且正在选举过程中，并且当前节点可用，则进行选举 leaderService.electLeader();
         * @author zhangliang
         */
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isActiveElection(path, data) || isPassiveElection(path, eventType))) {
                leaderService.electLeader();
            }
        }
        
        /**
         *如果没有主节点，并且判断给定路径是否是本地local路径，如果都为true的话，说明正在选举中。。。。
         * 
         * 
         * @author zhangliang
         */
        private boolean isActiveElection(final String path, final String data) {
            return !leaderService.hasLeader() && isLocalServerEnabled(path, data);
        }
        
        /**
         * 判断当前节点是否可用，如果没有被移除，并且对应的job实例的ip是否可用
         * 
         * @author zhangliang
         */
        private boolean isPassiveElection(final String path, final Type eventType) {
            return isLeaderCrashed(path, eventType) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp());
        }
        
        /**
         *判断给定路径leader/election/instance路径是否被移出，如果是移除的话，这里的eventype状态则为Type.NODE_REMOVED
         * 
         * @author zhangliang
         */
        private boolean isLeaderCrashed(final String path, final Type eventType) {
            return leaderNode.isLeaderInstancePath(path) && Type.NODE_REMOVED == eventType;
        }
        
        /**
         *判断给定路径是否是本地local路径，并且判断其状态是否是disabled，是本地且不是disabled则返回true
         * 
         * @author zhangliang
         */
        private boolean isLocalServerEnabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && !ServerStatus.DISABLED.name().equals(data);
        }
    }
    
    
    /**
     *当节点变化的时候触发，如果当前节点是主节点，并且当前节点的状态为disabled，则需要移除该节点，进行重新选举
     * @author zhangliang
     */
    class LeaderAbdicationJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (leaderService.isLeader() && isLocalServerDisabled(path, data)) {
                leaderService.removeLeader();
            }
        }
        
        private boolean isLocalServerDisabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && ServerStatus.DISABLED.name().equals(data);
        }
    }
}
