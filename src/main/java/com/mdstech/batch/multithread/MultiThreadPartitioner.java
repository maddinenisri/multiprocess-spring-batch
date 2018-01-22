package com.mdstech.batch.multithread;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MultiThreadPartitioner implements Partitioner {

    private Resource[] resources;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> results = new HashMap<>();
        int index = 0;
        for(Resource resource : resources) {
            ExecutionContext exContext = new ExecutionContext();
            exContext.put("name", "Thread" + index);
            exContext.put("file", resource.getFilename());
            results.put("partition" + index, exContext);
            index++;
        }
        return results;
    }

    public Resource[] getResources() {
        return resources;
    }

    public void setResources(Resource[] resources) {
        this.resources = resources;
    }
}
