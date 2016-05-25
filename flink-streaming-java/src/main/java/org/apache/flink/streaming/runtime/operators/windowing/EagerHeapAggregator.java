package org.apache.flink.streaming.runtime.operators.windowing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import com.google.common.collect.Sets;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Objects.requireNonNull;


import java.util.*;

/**
 * A full aggregator computes all pre-aggregates per partial result added. This implementation yields
 * the best aggregateFrom performance but it also has a heavy add operation. It is based on the FlatFat implementation
 * described by Kanat et al. in "General Incremental Sliding-Window Aggregation" published in VLDB 15. It encodes window
 * pre-aggregates in a binary tree using a fixed-size circular heap with implicit node relations.
 * <p/>
 * Node relations follow a typical heap structure. The heap is structured as such for example:
 * <p/>
 * | root | left(root) | right(root) | left(left(root)) ... | P1 | P2 | P3 | ... | Pn |
 * Back      Head
 * <p/>
 * The leaf space allocated for partials Pi lies within n-1 and 2n-2 indexes.
 * Furthermore we maintain a front and back pointer that circulate within the leaf space to mark the current
 * partial buffer. We always add new elements on the back and remove from the front in FIFO order.
 * <p/>
 * <p/>
 * The space complexity of this implementation is, for n partial aggregates 2n-1. Furthermore, it also keeps an index of
 * n partial aggregate ids.
 *
 * @param <T>
 */
public class EagerHeapAggregator<T> implements WindowAggregator<T> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	private static final Logger LOG = LoggerFactory.getLogger(EagerHeapAggregator.class);
    private final ReduceFunction<T> reduceFunction;
    private final TypeSerializer<T> serializer;
    private final T identityValue;


    private Map<Integer, Integer> leafIndex;
    /**
     * We use a fixed size list for the circular heap. We did not use an array due to the usual generics
     * issue in Java. Performance should be comparably reasonable using an ArrayList implementation.
     */
    private List<T> circularHeap;

    private int numLeaves;
    private int back, front;
    private final int ROOT = 0;

    private AggregationStats stats = AggregationStats.getInstance();

    private enum AGG_STATE {UPDATING, AGGREGATING}

    private AGG_STATE currentState = AGG_STATE.UPDATING;
    //private String lastRemove;


    /**
     * @param reduceFunction
     * @param serializer
     * @param identityValue  as the identity value (i.e. where reduce(defVal, val) == reduce(val, defVal) == val)
     * @param capacity
     */
    public EagerHeapAggregator(ReduceFunction<T> reduceFunction, TypeSerializer<T> serializer, T identityValue, int capacity) {
        if (((capacity & -capacity) != capacity))
            throw new IllegalArgumentException("Capacity should be a power of two");
        this.reduceFunction = requireNonNull(reduceFunction);
        this.serializer = serializer;
        this.identityValue = identityValue;
        this.numLeaves = capacity;
        this.back = capacity - 2;
        this.front = capacity - 1;
        this.leafIndex = new LinkedHashMap<Integer, Integer>(capacity);

        int fullCapacity = 2 * capacity - 1;
        stats.registerBufferSize(fullCapacity);
        this.circularHeap = new ArrayList<T>(Collections.nCopies(fullCapacity, identityValue));
        //lastRemove="";
    }

  
	@Override
    public void add(int partialId, T partialVal) throws Exception {
        add(partialId, partialVal, true);
    }

    /**
     * It adds the given value in the leaf space and if commit is set to true it materializes all partial pre-aggregates
     *
     * @param partialId
     * @param partialVal
     * @param commit
     * @throws Exception
     */
    private int add(int partialId, T partialVal, boolean commit) throws Exception {
        currentState = AGG_STATE.UPDATING;
        if (currentCapacity() == 0) {
            resize(2 * numLeaves);
        }
        incrBack();
        //lastRemove+="ADD: partialid: "+partialId+" position: "+back+" circularheap: "+circularHeap.size()+" Front:"+front+"\n";
        leafIndex.put(partialId, back);
        circularHeap.set(back, serializer.copy(partialVal));
        if (commit) {
            update(back);
        }

        return back;
    }

    /**
     * It reconstructs the heap with a new leaf space of size newCapacity
     *
     * @param newCapacity
     */
    private void resize(int newCapacity) throws Exception {
       // LOG.info("RESIZING HEAP TO {}", newCapacity);
    	//lastRemove+="Resize: "+newCapacity+"\n";
        int fullCapacity = 2 * newCapacity - 1;
        List<T> newHeap = new ArrayList<T>(Collections.nCopies(fullCapacity, identityValue));
        Integer[] updated = new Integer[leafIndex.size()];
        int indx = newCapacity - 2;
        int updateCount = 0;
        for (Map.Entry<Integer, Integer> entry : leafIndex.entrySet()) {
            newHeap.set(++indx, circularHeap.get(entry.getValue()));
            entry.setValue(indx);
            updated[updateCount++] = indx;
        }
        this.numLeaves = newCapacity;
        this.back = indx;
        this.front = newCapacity - 1;
        this.circularHeap = newHeap;
        update(updated);
//        lastRemove+="Resize: "+newCapacity+" front: "+front+" back: "+back+"\n";
//        lastRemove+="leafIndex"+"\n";
//     	for (Integer integer : leafIndex.keySet()) {
//     		lastRemove+="Partial: "+integer+" LeafID: "+leafIndex.get(integer)+"\n";
//		}
        stats.registerBufferSize(fullCapacity);
    }

    @Override
    public void add(List<Integer> ids, List<T> vals) throws Exception {
        currentState = AGG_STATE.UPDATING;
        if (ids.size() != vals.size()) throw new IllegalArgumentException("The ids and vals given do not match");

        //if we have reached max capacity (numLeaves) resize so that i*numLeaves > usedSpace+newSpace
        if (ids.size() > currentCapacity()) {
            int newCapacity = numLeaves;
            while (newCapacity < numLeaves - currentCapacity() + ids.size()) {
                newCapacity = 2 * newCapacity;
            }
            resize(newCapacity);
        }

        //add all new leaf values and perform a bulk update
        Integer[] updatedLeaves = new Integer[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            updatedLeaves[i] = add(ids.get(i), vals.get(i), false);
        }
        update(updatedLeaves);
    }

    @Override
    public void remove(Integer... partialList) throws Exception {
        currentState = AGG_STATE.UPDATING;
        List<Integer> leafBag = new ArrayList<Integer>(partialList.length);
        for (int partialId : partialList) {
            if (!leafIndex.containsKey(partialId)) continue;
            int leafID = leafIndex.get(partialId);
            leafIndex.remove(partialId);
            if (leafID != front) 
            	{
            	//System.out.println(lastRemove);
            	//System.out.println("=================Exception=================");
            	//System.out.println("Exception, partialId:"+partialId+ " LeafIndex: "+leafID+" Front: "+front);
            	
            	throw new IllegalArgumentException("Cannot evict out of order");
            	}
            circularHeap.set(front, identityValue);
            incrFront();
            leafBag.add(leafID);
        }
        update(leafBag.toArray(new Integer[leafBag.size()]));
        // shrink to half when the utilization is only one quarter
        if (currentCapacity() > 3 * numLeaves / 4 && numLeaves >= 4) {
        	//lastRemove+="resize: "+(numLeaves / 2)+"\n";
        	resize(numLeaves / 2);
        }
       // lastRemove+="After remove: "+" Front: "+front+" Back: "+back+"\n";
    }

    @Override
    public void removeUpTo(int id) throws Exception {
        currentState = AGG_STATE.UPDATING;
        List<Integer> toRemove = new ArrayList<Integer>();
        if (leafIndex.containsKey(id)) {
            for (Map.Entry<Integer, Integer> mapping : leafIndex.entrySet()) {
                if (mapping.getKey() == id)
                    break;
                toRemove.add(mapping.getKey());
            }
        }
//        if(id!=10)
//        {
//        	lastRemove+="=================RemoveUpTo=================";
//        	 lastRemove+="Last partial: "+id+"\n";
//             for (Integer integer : toRemove) {
//             	lastRemove+="rPartial: "+integer+" leafID: "+leafIndex.get(integer)+"\n";
//     		}
//            lastRemove+="leafIndex"+"\n";
//         	for (Integer integer : leafIndex.keySet()) {
//         		lastRemove+="Partial: "+integer+" LeafID: "+leafIndex.get(integer)+"\n";
//			}
//             
//        }
       
        
        remove(toRemove.toArray(new Integer[toRemove.size()]));
    }

    @Override
    public T aggregate(int partialId) throws Exception {
        currentState = AGG_STATE.AGGREGATING;
        if (leafIndex.containsKey(partialId)) {
            return aggregateFrom(leafIndex.get(partialId));
        }
        // in case no partials are registered (can be true if we trigger in the very beginning) return the identity
        return identityValue;
    }

    @Override
    public T aggregate() throws Exception {
        return aggregateFrom(front);
    }

    @Override
	public T aggregate(int startid, int endid) throws Exception {
    	if(startid>endid)
    	{
    		throw new Exception("the start partial ID cannot be greater than the end partial ID");
    	}
    	
    	currentState = AGG_STATE.AGGREGATING;
         if (leafIndex.containsKey(startid)&&leafIndex.containsKey(endid)) {
             return aggregateFromTo(leafIndex.get(startid),leafIndex.get(endid));
         }
         // in case no partials are registered (can be true if we trigger in the very beginning) return the identity
         return identityValue;
	}

	/**
     * Applies eager bulk pre-aggregation for all given mutated leafIDs. 
     * This works exactly as described in the RA paper
     */
    private void update(Integer... leafIds) throws Exception {
        Set<Integer> next = Sets.newHashSet(leafIds);
        do {
            Set<Integer> tmp = new HashSet<Integer>();
            for (Integer nodeId : next) {
                if (nodeId != ROOT) {
                    tmp.add(parent(nodeId));
                }
            }
            for (Integer parent : tmp) {
                circularHeap.set(parent, combine(circularHeap.get(left(parent)), circularHeap.get(right(parent))));
            }
            next = tmp;
        } while (!next.isEmpty());
    }

    /**
     * It invokes a reduce operation on copies of the given values
     * @param val1
     * @param val2
     * @return
     * @throws Exception
     */
    private T combine(T val1, T val2) throws Exception {
        switch (currentState) {
            case UPDATING:
                stats.registerUpdate();
                break;
            case AGGREGATING:
                stats.registerAggregate();
        }
        //System.out.println("Reduce Function: "+reduceFunction);
        //System.out.println("serializer: "+serializer);

        return reduceFunction.reduce(serializer.copy(val1), serializer.copy(val2));
    }

    /**
     * It collects an aggregated result starting from the leafID given until the back index of the circular heap
     *
     * @param leafID
     * @return
     * @throws Exception
     */
    private T aggregateFrom(int leafID) throws Exception {
        currentState = AGG_STATE.AGGREGATING;
        if (back < leafID) {
            return combine(prefix(back), suffix(leafID));
        }

        return (leafID == front) ? circularHeap.get(ROOT) : suffix(leafID);
    }
    
    /**
     * It collects an aggregated result starting from the leafID given until the back index of the circular heap
     *
     * @param leafID
     * @return
     * @throws Exception
     */
    private T aggregateFromTo(int startLeafID, int endLeafID) throws Exception {
        currentState = AGG_STATE.AGGREGATING;
        
        if (endLeafID < startLeafID) {
            return combine(prefix(endLeafID), suffix(startLeafID));
        }

        return (startLeafID == endLeafID) ? circularHeap.get(startLeafID) : suffix(startLeafID,endLeafID);
    }

    /**
     * it collects an aggregated result starting from the startleafID given until the endleafID
     *
     * @param startLeafID
     * @param endLeafID
     * @return
     * @throws Exception
     */
    private T suffix(int startLeafID, int endLeafID) throws Exception {
        int nextS = startLeafID;
        int nextE = endLeafID;
        
        T aggS = circularHeap.get(startLeafID);
        T aggE = circularHeap.get(endLeafID);
        
        while (nextS != ROOT&&nextS!=nextE) {
            int pS = parent(nextS);
            int pE = parent(nextE);
           
            if(pS!=pE)
            {
            		if (nextS == left(pS)) {
                    aggS = combine(aggS, circularHeap.get(right(pS)));
            		}
            	
            	if (nextE == right(pE)) {
                		aggE = combine(aggE, circularHeap.get(left(pE)));
                }
//            	if(leafIndex.containsValue(nextE))
//            	{
//        		}
//        	else
//        	{
//        		aggE = combine(aggE, circularHeap.get(right(pE)));
//        	}

            }
            nextS = pS;
            nextE = pE;
        }

        return combine(aggS,aggE);
    }
    
    /**
     * it collects an aggregated result starting from the leafID given until the end of the leaf space
     *
     * @param leafID
     * @return
     * @throws Exception
     */
    private T suffix(int leafID) throws Exception {
        int next = leafID;
        T agg = circularHeap.get(next);
        while (next != ROOT) {
            int p = parent(next);
            if (next == left(p)) {
                agg = combine(agg, circularHeap.get(right(p)));
            }

            next = p;
        }

        return agg;
    }

    /**
     * it collects an aggregated result from the beginning of the leaf space to the leafID given
     *
     * @param leafId
     * @return
     * @throws Exception
     */
    private T prefix(int leafId) throws Exception {
        int next = leafId;
        T agg = circularHeap.get(next);
        while (next != ROOT) {
            int p = parent(next);
            if (next == right(p)) {
                agg = combine(circularHeap.get(left(p)), agg);
            }

            next = p;
        }

        return agg;
    }

    /**
     * It returns the parent of nodeID from the heap space
     *
     * @param nodeId
     * @return
     */
    private int parent(int nodeId) {
        return (nodeId - 1) / 2;
    }

    /**
     * It returns the left child of the nodeID given
     *
     * @param nodeId
     * @return
     */
    private int left(int nodeId) {
        return 2 * nodeId + 1;
    }

    /**
     * It returns the right child of the nodeID given
     *
     * @param nodeId
     * @return
     */
    private int right(int nodeId) {
        return 2 * nodeId + 2;
    }

    /**
     * It moves the back pointer of the leaf space forward in the circular buffer
     */
    private void incrBack() {
        back = ((back - numLeaves + 2) % numLeaves) + numLeaves - 1;
    }

    /**
     * It moves the front pointer of the leaf space forward in the circular buffer
     */
    private void incrFront() {
        front = ((front - numLeaves + 2) % numLeaves) + numLeaves - 1;
    }

    /**
     * @return the current number of free slots in the leaf space
     */
    public int currentCapacity() {
        int capacity = numLeaves - leafIndex.size();
       // LOG.info("CURRENT CAPACITY : {}", capacity);
        return capacity;
    }

	@Override
	public void update(int id, T val) throws Exception {
	
		 currentState = AGG_STATE.UPDATING;
	        if (currentCapacity() == 0) {
	            resize(2 * numLeaves);
	        }
	        //incrBack();
	        int back_id=leafIndex.get(id);
	        T current_val=circularHeap.get(back_id);
	        
	        circularHeap.set(back_id, reduceFunction.reduce(serializer.copy(current_val), val));
	       
	        update(back_id);
		
	}


	@Override
	public int getNumberOfPartials() {
		// TODO Auto-generated method stub
		return numLeaves;
	}

}