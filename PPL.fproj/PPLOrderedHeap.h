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
 
    PPLOrderedHeap.h
    Ordered binary heap

    Copyright 1994-1998, Apple Computer, Inc. All rights reserved.
*/

#import <Foundation/NSZone.h>

/* A ordered-heap contains ordered objects, with the following operations:
	insertion 				O(log n)
	access to the top (smallest element)	O(1)
	removal of the top			O(log n)
    Note that there is no cheap random access, but you can use a head to sort items in O(n * log n) and then have random access in O(log n) 
*/

typedef struct _PPLOrderedHeap {
    int		(*compare)(struct _PPLOrderedHeap *, void *, void *);
    unsigned	count;
    unsigned	max;		/* private */
    void	**array;	/* private */
} PPLOrderedHeap;

typedef int PPLOrderedHeapComparator(PPLOrderedHeap *, void *, void *);
    /* Comparison should return <0 if ascending >0 if descending 0 if same */

FOUNDATION_EXPORT PPLOrderedHeap *PPLCreateOrderedHeapFromZone(PPLOrderedHeapComparator *compare, unsigned capacity, NSZone *zone);
FOUNDATION_EXPORT PPLOrderedHeap *PPLCreateOrderedHeap(PPLOrderedHeapComparator *compare, unsigned capacity);
    /* Creates a heap;
    if NULL compare is given, uses integer comparison;
    capacity is just a hint; 0 fine */
    
FOUNDATION_EXPORT void PPLFreeOrderedHeap(PPLOrderedHeap *heap);
    /* Frees entire heap;
    No function nor method is applied to the heap items */

FOUNDATION_EXPORT unsigned PPLCountOrderedHeap(PPLOrderedHeap *heap);
    /* The number of items in the heap. */

FOUNDATION_EXPORT void PPLAddToOrderedHeap(PPLOrderedHeap *heap, void *data);
    /* Adds an item into the heap; 
    it is OK to insert same data more than once;
    Executes in guaranteed O(log(N));
    When items are added in increasing order, execution is guaranteed O(1) */

FOUNDATION_EXPORT void PPLAddMultipleToOrderedHeap(PPLOrderedHeap *heap, void **data, unsigned count);
    /* Adds several items into the heap; 
    Similar to PPLAddToOrderedHeap() applied multiple times, just a little bit faster */

FOUNDATION_EXPORT void *PPLMinimumOfOrderedHeap(PPLOrderedHeap *heap);
    /* Returns the lowest item in heap;
    NULL is returned iff there is nothing in heap;
    Very fast: executes in O(1)! */

FOUNDATION_EXPORT void *PPLRemoveMinimumOfOrderedHeap(PPLOrderedHeap *heap);
    /* Removes min item and returns it;
    NULL is returned iff there is nothing in heap;
    Executes in O(log(N)) */

FOUNDATION_EXPORT void PPLRemoveMultipleFromOrderedHeap(PPLOrderedHeap *heap, void **data, unsigned count);
    /* Applies PPLRemoveMinimumOfOrderedHeap() count times, therefore filling data with growing items (or NULL) */

FOUNDATION_EXPORT void *PPLRemoveRandomInOrderedHeap(PPLOrderedHeap *heap);
    /* Removes some item and returns it;
    NULL is returned iff there is nothing in heap;
    Very fast: executes in O(1) */

FOUNDATION_EXPORT void *PPLRemoveLeastGreaterOfOrderedHeap(PPLOrderedHeap *heap, void *item);
    /* Removes the smallest item greater or equal than argument;
    Slow: executes in O(N)! */

