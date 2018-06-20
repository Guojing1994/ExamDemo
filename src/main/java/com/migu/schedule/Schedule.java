package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {
    // 服务节点,List存储运行的任务
    Map<Integer, List<Integer>> nodes;
    // 任务队列
    Map<Integer, Integer> taskQueue;
    // 执行中的任务,List存储格式为[nodeId, consumption]
    Map<Integer, List<Integer>> taskExecute;

    public int init() {
        nodes = new HashMap<Integer, List<Integer>>();
        taskQueue = new HashMap<Integer, Integer>();
        taskExecute = new HashMap<Integer, List<Integer>>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        } else if (nodes.get(nodeId) != null) {
            return ReturnCodeKeys.E005;
        }
        nodes.put(nodeId, new ArrayList<Integer>());
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        } else if (nodes.get(nodeId) == null) {
            return ReturnCodeKeys.E007;
        }
        List<Integer> tasks = nodes.get(nodeId);
        if (tasks.size() > 0) {
            for (int i = 0; i < tasks.size(); i++) {
                int taskId = tasks.get(i);
                if (taskExecute.get(taskId) != null) {
                    taskQueue.put(taskId, taskExecute.get(taskId).get(1));
                    taskExecute.remove(taskId);
                }
            }
            nodes.remove(nodeId);
        }
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        } else if (taskQueue.get(taskId) != null || taskExecute.get(taskId) != null) {
            return ReturnCodeKeys.E010;
        }
        taskQueue.put(taskId, consumption);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        } else if (taskQueue.get(taskId) == null && taskExecute.get(taskId) == null) {
            return ReturnCodeKeys.E012;
        }
        if (taskQueue.get(taskId) != null) {
            taskQueue.remove(taskId);
        } else if (taskExecute.get(taskId) != null) {
            nodes.remove(taskExecute.get(taskId).get(0));
            taskExecute.remove(taskId);
        }
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        if (threshold <= 0) {
            return ReturnCodeKeys.E002;
        }
        // 先取出所有任务、消耗率及总消耗率
        int consumptionRate = 0;
        List<Map<Integer, Integer>> tasks = new ArrayList<Map<Integer, Integer>>();
        for (Integer taskId : taskExecute.keySet()) {
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();
            map.put(taskId, taskExecute.get(taskId).get(1));
            tasks.add(map);
            consumptionRate += taskExecute.get(taskId).get(1);
        }
        for (Integer taskId : taskQueue.keySet()) {
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();
            map.put(taskId, taskQueue.get(taskId));
            tasks.add(map);
            consumptionRate += taskQueue.get(taskId);
        }
        // 计算出平均值
        int average = consumptionRate / nodes.size();
        // 先将任务按id从小到大排序，再按消耗率从大到小排序
        tasks.sort(new Comparator<Map<Integer, Integer>>() {
            public int compare(Map<Integer, Integer> o1, Map<Integer, Integer> o2) {
                int taskId1 = 0;
                int taskId2 = 0;
                for (Integer taskId : o1.keySet()) {
                    taskId1 = taskId;
                }
                for (Integer taskId : o2.keySet()) {
                    taskId2 = taskId;
                }
                return taskId1 > taskId2 ? 1 : -1;
            }
        });
        tasks.sort(new Comparator<Map<Integer, Integer>>() {
            public int compare(Map<Integer, Integer> o1, Map<Integer, Integer> o2) {
                int consumption1 = 0;
                int consumption2 = 0;
                for (Integer taskId : o1.keySet()) {
                    consumption1 = o1.get(taskId);
                }
                for (Integer taskId : o2.keySet()) {
                    consumption2 = o2.get(taskId);
                }
                return consumption1 > consumption2 ? -1 : 1;
            }
        });
        // 将节点按从小到大排序
        List<Integer> nodeIds = new ArrayList<Integer>();
        for (Integer nodeId : nodes.keySet()) {
            nodeIds.add(nodeId);
        }
        nodeIds.sort(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o1 > o2 ? 1 : -1;
            }
        });
        // 先给id小的节点分配消耗率大的任务，超出平均值则给下一个节点分配
        Map<Integer, Integer> simulationtaskQueue = new HashMap<Integer, Integer>();
        Map<Integer, List<Integer>> simulationtaskExecute = new HashMap<Integer, List<Integer>>();
        Map<Integer, List<Integer>> simulationnodes = new HashMap<Integer, List<Integer>>();
        for (Integer nodeId : nodeIds) {
            List<Integer> taskIds = new ArrayList<Integer>();
            int consumptions = 0;
            List<Map<Integer, Integer>> removeTasks = new ArrayList<Map<Integer, Integer>>();
            for (Map<Integer, Integer> map : tasks) {
                int id = 0;
                for (Integer taskId : map.keySet()) {
                    id = taskId;
                }
                int consumption = map.get(id);
                if ((consumptions + consumption) < average) {
                    consumptions += consumption;
                    taskIds.add(id);
                    removeTasks.add(map);
                    List<Integer> list = new ArrayList<Integer>();
                    list.add(nodeId);
                    list.add(consumption);
                    simulationtaskExecute.put(id, list);
                }
            }
            simulationnodes.put(nodeId, taskIds);
            tasks.removeAll(removeTasks);
        }
        int inxde = 0;
        for (int i = nodeIds.size() - 1; i >= 0; i--) {
            if (tasks.get(inxde) != null) {
                int id = 0;
                for (Integer taskId : tasks.get(inxde).keySet()) {
                    id = taskId;
                }
                List<Integer> taskIds = simulationnodes.get(nodeIds.get(i));
                taskIds.add(id);
                simulationnodes.put(nodeIds.get(i), taskIds);
            }
        }
        if (1 == 1) {

        }
        nodes = simulationnodes;
        taskQueue = simulationtaskQueue;
        taskExecute = simulationtaskExecute;
        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if (tasks == null) {
            return ReturnCodeKeys.E016;
        }
        for (Integer taskId : taskExecute.keySet()) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(taskId);
            taskInfo.setNodeId(taskExecute.get(taskId).get(0));
            tasks.add(taskInfo);
        }
        for (Integer taskId : taskQueue.keySet()) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(taskId);
            taskInfo.setNodeId(-1);
            tasks.add(taskInfo);
        }
        tasks.sort(new Comparator<TaskInfo>() {
            public int compare(TaskInfo o1, TaskInfo o2) {
                int taskId1 = o1.getTaskId();
                int taskId2 = o2.getTaskId();
                return taskId1 > taskId2 ? 1 : -1;
            }
        });
        return ReturnCodeKeys.E015;
    }

}
