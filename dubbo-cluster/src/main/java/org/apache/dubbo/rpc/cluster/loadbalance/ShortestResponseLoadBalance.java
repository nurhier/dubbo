/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ShortestResponseLoadBalance 最短响应负载均衡，重点是如何计算最短响应时长
 * </p>
 * Filter the number of invokers with the shortest response time of success calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 */
public class ShortestResponseLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "shortestresponse";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers // 服务提供者数量
        int length = invokers.size();
        // Estimated shortest response time of all invokers // 最短响应时间，临时变量
        long shortestResponse = Long.MAX_VALUE;
        // The number of invokers having the same estimated shortest response time
        int shortestCount = 0;
        // The index of invokers having the same estimated shortest response time
        int[] shortestIndexes = new int[length];
        // the weight of every invokers
        int[] weights = new int[length];
        // The sum of the warmup weights of all the shortest response  invokers
        int totalWeight = 0;
        // The weight of the first shortest response invokers
        int firstWeight = 0;
        // Every shortest response invoker has the same weight value?
        boolean sameWeight = true;

        // Filter out all the shortest response invokers
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()); // 根据服务提供者url和接口方法名称获取rpc url请求统计信息
            // Calculate the estimated response time from the product of active connections and succeeded average elapsed time.
            long succeededAverageElapsed = rpcStatus.getSucceededAverageElapsed(); // 获取成功请求平均耗时
            int active = rpcStatus.getActive(); // 服务提供者此方法url请求数
            long estimateResponse = succeededAverageElapsed * active; // 计算活跃请求预计消耗的时长
            int afterWarmup = getWeight(invoker, invocation);
            weights[i] = afterWarmup;
            // Same as LeastActiveLoadBalance // 预计消耗时长小于最短响应时长，则保存此invoker下标，并作为第一个可选invoker，权重作为初始权重
            if (estimateResponse < shortestResponse) {
                shortestResponse = estimateResponse;
                shortestCount = 1;
                shortestIndexes[0] = i;
                totalWeight = afterWarmup;
                firstWeight = afterWarmup;
                sameWeight = true;
            } else if (estimateResponse == shortestResponse) { // 如果预计消耗时长等于最小消耗时长，则保存invoker下标
                shortestIndexes[shortestCount++] = i;
                totalWeight += afterWarmup;
                if (sameWeight && i > 0
                        && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        if (shortestCount == 1) { // 如果只有一个最短响应的invoker，则直接选中
            return invokers.get(shortestIndexes[0]);
        }
        if (!sameWeight && totalWeight > 0) { // 如果有多个最短响应相同的invoker，则根据权重随机选择一个invoker
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < shortestCount; i++) {
                int shortestIndex = shortestIndexes[i];
                offsetWeight -= weights[shortestIndex];
                if (offsetWeight < 0) {
                    return invokers.get(shortestIndex);
                }
            }
        }
        return invokers.get(shortestIndexes[ThreadLocalRandom.current().nextInt(shortestCount)]); // 如果有多个最短响应相同且权重相同，则随机选择一个invoker
    }
}
