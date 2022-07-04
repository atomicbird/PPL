/*
    May 27, 1998
    Warning: These PPL sources are obsolete and unsupported.
 
    You may incorporate this sample code into your applications without
    restriction, though the sample code has been provided "AS IS" and the
    responsibility for its operation is 100% yours.  However, what you are
    not permitted to do is to redistribute the source as "Apple Sample Code"
    after having made changes. If you're going to re-distribute the source,
    we require that you make it clear in the source that the code was
    descended from Apple Sample Code, but that you've made changes. The use
    of the software is also governed by the software license agreement
    included in the ReadMe file.

    PPLOrderedHeap.m
    Ordered binary heap

    Copyright 1994-1998, Apple Computer, Inc. All rights reserved.
*/

#import <Foundation/Foundation.h>
#import "PPLOrderedHeap.h"

/* Credits to Louis Monier and others...
    Reference:	Data Structures and Network Algorithms by R.E. Tarjan
		CBMS-NSF Regional Conference Series in Applied Mathematics
		Philadelphia, Pennsylvania  1983
    An OrderedHeap is implemented as a binary tree containing one item per node, 
    arranged in heap order.  It is implemented on a dynamically allocated array.  
    If we number the nodes of a complete binary tree from 0 to size-1 in 
    breadth-first order and identify the nodes by number, then the 
    parent of node x is (x-1)/2), and the set of children of node x is the 
    interval [2*x+1.. 2*x+2] but these children might not be all present (if 2*(x+1) >= size).
    Basic idea for Insert: Insert as last element in the array, and exchange 
    it with its parent until it finds its place.
*/

static int compareInts(PPLOrderedHeap *heap, void *data1, void *data2) {
    return ((int)data1 - (int)data2);
}

PPLOrderedHeap *PPLCreateOrderedHeapFromZone(PPLOrderedHeapComparator *compare, unsigned capacity, NSZone *zone) {
    PPLOrderedHeap	*heap = NSZoneMalloc(zone, sizeof(PPLOrderedHeap));
    heap->count = 0;
    heap->compare = (compare) ? compare : compareInts;
    heap->max = capacity;
    heap->array = (capacity) ? NSZoneMalloc(zone, capacity * sizeof(void *)) : NULL;
    return heap;
}

PPLOrderedHeap *PPLCreateOrderedHeap(PPLOrderedHeapComparator *compare, unsigned capacity) {
    return PPLCreateOrderedHeapFromZone(compare, capacity, NULL);
}

void PPLFreeOrderedHeap(PPLOrderedHeap *heap) {
    NSZoneFree(NSZoneFromPointer(heap->array), heap->array);
    NSZoneFree(NSZoneFromPointer(heap), heap);
}

unsigned PPLCountOrderedHeap(PPLOrderedHeap *heap) {
    return heap->count;
}

FOUNDATION_STATIC_INLINE void bubbleUp(PPLOrderedHeap *heap, void *data, unsigned idx) {
    PPLOrderedHeapComparator	*compare = heap->compare;
    while (idx) {
	unsigned	parent = (idx-1) >> 1; /* parent */
	void		*pdata = heap->array[parent];
	int		comp;
	comp = (compare == compareInts) ? ((int)pdata - (int)data) : compare(heap, pdata, data);
	if (comp <= 0) break;
	/* swap with parent, i.e. bubble up */
	heap->array[idx] = pdata;
	idx = parent;
    }
    heap->array[idx] = data;
}

void PPLAddToOrderedHeap(PPLOrderedHeap *heap, void *data) {
    unsigned	idx = heap->count;
    if (heap->count == heap->max) {
	NSZone	*zone = NSZoneFromPointer(heap);
	heap->max += heap->max + 1; /* we double, essentially */
	heap->array = NSZoneRealloc(zone, heap->array, heap->max * sizeof(void *));
    }
    heap->count++;
    bubbleUp(heap, data, idx);
}

void PPLAddMultipleToOrderedHeap(PPLOrderedHeap *heap, void **data, unsigned count) {
    if (heap->count + count > heap->max) {
	NSZone	*zone = NSZoneFromPointer(heap);
	while (heap->count + count > heap->max) {
	    heap->max += heap->max + 1; /* we double, essentially */
	}
	heap->array = NSZoneRealloc(zone, heap->array, heap->max * sizeof(void *));
    }
    while (count--) {
	unsigned	idx = heap->count;
	heap->count++;
	bubbleUp(heap, *data, idx);
	data++;
    }
}

void *PPLMinimumOfOrderedHeap(PPLOrderedHeap *heap) {
    return (! heap->count) ? NULL : heap->array[0];
}

FOUNDATION_STATIC_INLINE void bubbleDown(PPLOrderedHeap *heap, void *moved, unsigned parent) {
    PPLOrderedHeapComparator	*compare = heap->compare;
    unsigned			child;
    while ((child = (parent << 1) + 1) < heap->count) { /* while there are children */
	/* child is first child */
	void		*data = heap->array[child];
	int		comp;
	/* we take the smallest of both children */
	if (child + 1 <= heap->count - 1) {
	    void	*data2 = heap->array[child + 1];
	    comp = (compare == compareInts) ? ((int)data - (int)data2) : compare(heap, data, data2);
	    if (comp > 0) {
		child++;
		data = data2;
	    }
	}
	/* child now is best child */
	/* bubble down */
	comp = (compare == compareInts) ? ((int)data - (int)moved) : compare(heap, data, moved);
	if (comp >= 0) break;
	heap->array[parent] = data;
	parent = child;
    }
    heap->array[parent] = moved;
}

void *PPLRemoveMinimumOfOrderedHeap(PPLOrderedHeap *heap) {
    void	*top;
    void	*moved;
    unsigned	parent = 0;
    if (! heap->count) return NULL;
    top = heap->array[0];
    heap->count--;
    if (! heap->count) return top;
    moved = heap->array[heap->count];
    bubbleDown(heap, moved, parent);
    return top;
}

void PPLRemoveMultipleFromOrderedHeap(PPLOrderedHeap *heap, void **data, unsigned count) {
    memset(data, 0, count * sizeof(void *));
    while (count--) {
	void		*moved;
	unsigned	parent = 0;
	if (! heap->count) return;
	*data = heap->array[0];
	heap->count--;
	if (heap->count) {
	    moved = heap->array[heap->count];
	    bubbleDown(heap, moved, parent);
	}
	data++;
    }
}

void *PPLRemoveRandomInOrderedHeap(PPLOrderedHeap *heap) {
    if (!heap->count) return NULL;
    heap->count--;
    return heap->array[heap->count];
}

static void findMinOfLarger(PPLOrderedHeap *heap, void *item, unsigned idx, unsigned *minIndex, void **min) {
    PPLOrderedHeapComparator	*compare = heap->compare;
    int		comp;
    void	*top;
    if (idx >= heap->count) return;
    top = heap->array[idx];
    if (*min) {
	comp = (compare == compareInts) ? ((int)(*min) - (int)top) : compare(heap, *min, top);
	if (comp < 0) return;
    }
    comp = (compare == compareInts) ? ((int)item - (int)top) : compare(heap, item, top);
    if (comp <= 0) {
	*min = top; *minIndex = idx;
	return;
    }
    findMinOfLarger(heap, item, (idx << 1) + 1, minIndex, min);
    findMinOfLarger(heap, item, (idx << 1) + 2, minIndex, min);
}

void *PPLRemoveLeastGreaterOfOrderedHeap(PPLOrderedHeap *heap, void *item) {
    void	*min = NULL; /* min larger than item found so far */
    unsigned	idx;
    void	*data;
    findMinOfLarger(heap, item, 0, &idx, &min);
    if (!min) return NULL;
    data = heap->array[--heap->count];
    bubbleDown(heap, data, idx);
    data = heap->array[idx];
    /* just like in PPLAddToOrderedHeap, we now bubble up element at idx */
    bubbleUp(heap, data, idx);
    return min;
}

