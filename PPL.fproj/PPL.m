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
	
    PPL.m
    Persistent Property List

    Copyright 1993-1998, Apple Computer, Inc. All rights reserved.
*/

#import <Foundation/Foundation.h>
#import <PPL/PPL.h>
#import "PPLOrderedHeap.h"
#import <sys/stat.h>
#if defined(WIN32)
#import <fcntl.h>
#else
#import <sys/fcntl.h>
#endif

#define PPLLog NSLog

NSString * const PPLDidBecomeDirtyNotification = @"PPLDidBecomeDirtyNotification";
NSString * const PPLDidSaveNotification = @"PPLDidSaveNotification";

@class _PPLStore, _SortedNums, _PPLReadOnlyStore, _PPLMutableDictEnum, _PPLCache;

/***************	PPL Debug and Bench	***************/

@interface PPL (PPLDebugExtras)

- (_PPLCache *)_cache;

- (_PPLStore *)_store;

- (NSString *)_specialDump;
    /* Special dump allowing manual fix of broken trees */

+ (void)_setTestRaiseCount:(unsigned)counter;

+ (void)_setCopyAfterSave:(BOOL)enable;
    // if YES a copy is made after each save (quite slow)

+ (id)_lastCopyAfterSave;
    // last saved copy

- (void)_raiseFormat:(NSString *)format, ...;
    // should be called whenever a file inconsistency is detected so that high level client can know which PPL is invalid and act (e.g. destroy it when it is a cache)

@end

/***************	Error / Debug utilities		***********/

#define _PPLTestException	@"_PPLTestCrash"

#define _PPLTestPostAppendingDataBeforeSync	@"post appending data changes, before sync"
#define _PPLTestPostAppendingDataAfterSync	@"post appending data changes, after sync"

#define _PPLTestPostAppendingLogChangesBeforeSync	@"post appending log changes, before sync"
#define _PPLTestPostAppendingLogChangesAfterSync	@"post appending log changes, before sync"

#define _PPLTestPostAppendingLogMarkerBeforeSync	@"post appending log marker, before sync"
#define _PPLTestPostAppendingLogMarkerAfterSync	@"post appending log marker, after sync"

static int _PPLRaiseCount;

static void _PPLTestAndRaise(NSString *state);

/***************	File Utilities		***********/

static void _PPLMoveToBackup(NSString *path);

/***************	Misc		***********/

@interface NSMutableString (_PPLMutableStringExtras)

- (void)appendStoreHandle:(unsigned)handle;
    /* For consistent descriptions */

@end

static id _PPLRetrieve(PPL *self, unsigned handle, BOOL cache);
    /* returns a stored value (most times);
    When cache is NO and we are retrieving a string, may return the actual string
    A function, not a method, for efficiency (critical in this case) */

static id _PPLRetrieveCompact(PPL *self, unsigned origin, unsigned idx, BOOL flag);
    /* functionally equivalent to _PPLRetrieve(), but compacts forwarding pointers */

static unsigned _PPLFollowForwardingHandles(PPL *self, unsigned handle);
    /* Compacts all the way */

static NSRange _PPLRoundUpRange(NSRange range, unsigned rounding);
    // rounds a range to the smallest range multiple of rounding containing range
    // rounding must be a power of 2
    
/***********		Sorted Int Array		***********/

@interface _SortedNums:NSObject {
    unsigned	count;
    unsigned	max;
    unsigned	*nums;
}

- initWithCapacity:(unsigned)capa;

- (void)addUnsigned:(unsigned)item;

- (void)removeUnsigned:(unsigned)item;

- (unsigned)smallestGreaterOrEqual:(unsigned)item;
    /* 0 iff none found */

- (unsigned)largestLesser:(unsigned)item;
    /* 0 iff none found */

- (unsigned)removeSmallest;
    /* 0 iff none */

- (unsigned)removeLargest;
    /* 0 iff none */

@end

/***********		Range Arrays		***********/

typedef struct __PPLRangeArray {
    // public struct; callers can freely peek and poke
    unsigned	count;
    unsigned	max;
    NSRange	*ranges;
} _PPLRangeArray;

static _PPLRangeArray *CreatePPLRangeArrayWithZone(unsigned capacity, NSZone *zone);

static _PPLRangeArray *CreatePPLRangeArray(unsigned capacity);

static void FreePPLRangeArray(_PPLRangeArray *ranges);

typedef enum {
    _PPLRangeArrayAppend = 0,
    _PPLRangeArrayAddToHeap	// consider as heap sorted by location
} _PPLRangeArrayAddPolicy;
 
static void AddToPPLRangeArray(_PPLRangeArray *ranges, NSRange range, _PPLRangeArrayAddPolicy policy);
    // takes care of the growth, merging (when applicable), etc...

typedef enum {
    _PPLRangeArrayRemoveMinimumFromHeap	// consider as heap sorted by location
} _PPLRangeArrayRemoveMinimumPolicy;

static NSRange RemoveMinimumOfPPLRangeArray(_PPLRangeArray *ranges, _PPLRangeArrayRemoveMinimumPolicy policy);
    // Returns the smallest range, {0, 0} iff none

/***********	 String for PPL purposes		***********/

@interface _PPLString:NSString {
    // notifies cache when going away
    NSString *string;
    unsigned	extraRefCount;
}
- initWithString:(NSString *)str;
@end

typedef struct {
    id		object;
    unsigned	handle;
    float	lastAccessTime; // relative to initialDate, >0
} _PPLCacheEntry;

@interface _PPLCache:NSObject {
    // essentially, the cache consists of a two way table mapping handles to objects (and vice versa); Also the date the entry was last refreshed is there, as a displacement from the initialDate (in order to avoid expensive date computations).
    unsigned		count;
    unsigned		max;
    _PPLCacheEntry	**oentries; // buckets, hashed per object
    _PPLCacheEntry	**hentries; // buckets, hashed per handle
    NSDate 		*initialDate;
    BOOL		shouldRefresh; // if YES, refresh is due
    float		currentGenerationTime; // relative to initialDate, >0
    unsigned		currentGenerationCounter;
    NSTimeInterval	halfLife;
}

+ (void)noteDeallocatingProxy:object;
    /* event notification - just faster */

- (void)setHalfLife:(NSTimeInterval)halfLife;
    /* 0 means whenever refreshing, get rid of cache entry */

- (unsigned)cachedHandleForObject:object;
    /* Fast, O(1) */

- cachedObjectForHandle:(unsigned)handle;
    /* Fast, O(1) */

- (void)addCachedObject:object handle:(unsigned)handle;
    /* Fast, O(1) on average */

- (void)removeCachedObject:object;
    /* Somewhat fast */

- (void)removeCachedObjectForHandle:(unsigned)handle;
    /* Somewhat fast */

- (void)removeAllCachedObjects;
    /* empties the cache; slow */

- (unsigned)count;

- (void)getAllCachedObjectsInto:(id *)objects handles:(unsigned *)handles max:(unsigned)max;
    /* Cannot return an NSArray (to avoid retain) */

- (void)refreshCache;
    /* Throws away old entries; slow */ 

@end

/***************	Types for specific Sorted Lists	***********/

typedef struct {
    unsigned	num;
    void	*data;
} NumToVoid;

typedef struct {
    unsigned	count;
    unsigned	max;
    NumToVoid	*pairs;
} SortedNumToVoids;

/***************	Free Information	***************/

@interface _PPLFreeInfo:NSObject {
    _SortedNums	*free1;	/* free blocks, sorted of size MIN_CHUNK */
    SortedNumToVoids	*freeLengthToHeap; /* [free length -> heap of addresses] */
    SortedNumToVoids	*freeToLength; /* [free -> its length] */
}
- (unsigned)noteFree:(unsigned)handle inStore:(_PPLStore *)store;
    /* tries to merge; 
    returns the handle after handle, all merges being done */
    
- (unsigned)findFreeBlock:(unsigned)wantedLength cold:(BOOL)cold inStore:(_PPLStore *)store;
    /* wantedLength must have been rounded up;
    if cold towards the cold end, else at random */
    
@end

typedef enum {
    LogEntryMarker = 0,
    LogEntryChange,
    LogEntryFree,
    LogEntryInvalid	// Never on file, used internally
} LogEntryType;

typedef struct {
    LogEntryType	type;
    unsigned		address;
    unsigned		num0;
    unsigned		num1;
    unsigned		num2;
    unsigned		num3;
} LogEntry;

typedef enum {
    LogNormal,			// log empty and consistent with data
    LogTruncated,		// log was applied: only a truncation needed
    LogScavenged,		// log was applied; full scavenging
    LogUnsuccessfullyApplied,	// log was applied; errors encountered
    LogUnusable			// log totally unusable
} RecoveryType;

#define _PPLMaxLogged	1024

@interface _PPLLogInfo:NSObject {
    NSFileHandle *logfd;
    				/* open during a transaction */
				/* Set to nil when a fatal error occured */
    unsigned long		lastLengthSyncedLog;	/* length when last synced */
    _PPLRangeArray	*logged;		/* somewhat sorted */
    unsigned		sizeOfChanged;	/* the last time it was computed */
    unsigned		approximateSinceLastCompute; 	/* approximate! */
    unsigned		numToBeLogged;
    LogEntry		toBeLogged[_PPLMaxLogged];
}

+ (RecoveryType)recoverLogForPath:(NSString *)path applyToData:(NSMutableData *)current;
    // When recovery fails, mutable data is untouched

- initWithExistingLogForPath:(NSString *)path;

- initAndRefreshLogForPath:(NSString *)path;

- (BOOL)appendLogChangeAt:(unsigned)address original:(unsigned)original;
    /* returns YES if log was written */

- (BOOL)appendLogChangesAt:(unsigned)address original4:(unsigned[4])original;
    /* returns YES if log was written */

- (BOOL)appendLogFreeAt:(unsigned)address length:(unsigned)length;
    /* returns YES if log was written */

- (void)appendLogMarkerAndSync:(unsigned long long)lastLengthSyncedData;

- (_PPLRangeArray *)flushLogAndRangesOfChanges;
    /* Returns sorted ranges;
    ranges are sorted to diminish disk operations;
    Note that NULL maybe returned (no range).
    Otherwise caller should deallocate returned value; */
    
- (void)truncateLogAndMark:(unsigned long long)lastLengthSyncedData;

- (unsigned)sizeOfAllPageChangedAndLogged;
    /* somewhat expensive to compute; a hint only (can be wrong in order to be fast) */

@end

/***************	Definitions		***************/

#define STORED_FREE		0
#define STORED_FORWARD		1
#define STORED_MARRAY		2
#define STORED_MDICT		3
#define STORED_NSSTRING		4	// NEXTSTEP strings only; obsolete -- never written
#define STORED_IOBJECT		5

/***************	Mutable proxies		***************/

@protocol _PPLMutableObject
    /* Copying is not part of the stored object protocol, since copies should be real (i.e. non-stored) objects, so that one can safely include them elsewhere */

- (PPL *)_ppl;
    /* nil indicates a Zombie */

- (unsigned)_handle;

- (void)_destroySubsInStorage;
    /* called when a block is destroyed to let objects pointed to from that block destroy their dependents in turn */ 
    
- (unsigned)_inStoreRefCount;
    /* for debug and consistency checks only */

- (void)_zombifyInPPL:(PPL *)ppl;

@end

@interface _PPLMutableArray:NSMutableArray <_PPLMutableObject> {
    unsigned	inMemoryRefcount;
    PPL	*ppl;
    unsigned	handle;
    unsigned	count;
    unsigned	max;
}

+ (unsigned)createArrayBlockInStore:(_PPLStore *)store max:(unsigned *)maxi;

- initWithPPL:(PPL *)appl forHandle:(unsigned)hh;
    /* Note that appl is not retained */

@end

@interface _PPLMutableDict:NSMutableDictionary <_PPLMutableObject> {
    unsigned	inMemoryRefcount;
    PPL	*ppl;
    unsigned	handle;
    unsigned	count;
    unsigned	numBuckets; /* number of pairs (key specif, value specif) */
    unsigned	numEnumerators;
    _PPLMutableDictEnum	**enumerators;
}

+ (unsigned)createDictBlockInStore:(_PPLStore *)store capacity:(unsigned) capacity;

- initWithPPL:(PPL *)appl forHandle:(unsigned)hh;
    /* Note that appl is not retained */

- (void)_hardenEnumerators;
    /* Should be called whenever the dictionary is modified */

- (void)_removeEnumerator:(_PPLMutableDictEnum *)enumerator;
    /* Should be called whenever the enumerator is released;
    note that this may release self */

@end

@interface NSObject (PPLCallBack)

- (void)_freezeForHandle:(unsigned)handle inPPL:(PPL *)ppl;

@end

/***************	Immutable proxies		***************/

@interface _PPLPropertyList:NSProxy <NSCopying> {
    /* For serialized, immutable, stored objects */
    id		real;
}

- initWithRealPropertyList:plist;

@end

/***************	Immutable PPL Data proxies		***********/

@interface _PPLDataArray:NSArray {
    _PPLReadOnlyStore	*store;
    unsigned	handle;
    unsigned	count;
}

- initWithPPLDataStore:(_PPLReadOnlyStore *)store forHandle:(unsigned)handle;
    /* store is retained */

@end

@interface _PPLDataDictionary:NSDictionary {
    _PPLReadOnlyStore	*store;
    unsigned	handle;
    unsigned	count;
    unsigned	numBuckets; /* number of pairs (key specif, value specif) */
}

+ (NSDictionary *)retrieveRootDataStore:(_PPLReadOnlyStore *)store forHandle:(unsigned)handle;
    // forward forwarding pointers

- initWithPPLDataStore:(_PPLReadOnlyStore *)store forHandle:(unsigned)handle;
    /* store is retained */

@end

@interface _PPLMutables:NSObject {
    NSMapTable		*mobjectToHandle;
	    // mutable object -> its handle
    NSMapTable		*handleToMObject; // handle -> mutable object
    	// all mutable objects returned in transaction MUST be in handleToMObject
	// logically 'owns' objects
    NSHashTable		*handlesToDestroy;
    	// set of all handles to destroy on flush or save
}

- (BOOL)isEmpty;

/***************	Handles to objects	***************/

- (void)noteHandle:(unsigned)mhandle forMutableObject:(id)mutableObject;
    // retains

- (void)noteHandle:(unsigned)mhandle forMutableObjectNonRetained:mutableObject;
    // same as previous but does not retain for efficiency (caller must do it!)

- (id)mutableObjectForHandle:(unsigned)mhandle;

- (unsigned)handleForMutableObject:(id)value;
    /* return 0 if not a mutable object to freeze */

- (unsigned)getHandlesToMutableObjectsInto:(unsigned **)refHandles;
    /* sets refHandles (that must be freed by caller) and returns count */

- (BOOL)forgetMutableObjectHandle:(unsigned)mhandle;
    /* returns YES if it was indeed a handle in table */

- (unsigned)mutableObjectsCount;

- (BOOL)_hasNonProxyMutable;
    /* Returns YES if there are tru mutable arrays of dicts */

/***************	Objects to destroy	***************/

- (void)noteMutableHandleToDestroy:(unsigned)mhandle;

- (BOOL)forgetMutableHandleToDestroy:(unsigned)mhandle;
    /* returns YES if mhandle was on the toDestroy set */

- (unsigned)getHandlesToDestroyInto:(unsigned **)refHandles;
    /* sets refHandles (that must be freed by caller) and returns count  */

- (unsigned)handleToDestroyCount;

@end

/***************	Definitions	***************/

#define WORD_SIZE		(sizeof(unsigned))

#define MIN_CHUNK	(4 * WORD_SIZE)	// changing this changes file format
	// must be at least BLOCK_HEADER_LEN to avoid fragmentation when reusing free blocks

#define BLOCK_HEADER_LEN	(2 * WORD_SIZE)

#define LARGEST_REFCOUNT	0x00ffffff

typedef struct {
    unsigned	refCount;	/* limited to 24 bits */
    unsigned	type;		/* limited to 8 bits */
    unsigned	capacity;	/* number of useful bytes */
} BlockHeader;

@protocol PPLTypeProvider

- (BOOL)checkValidHeader:(BlockHeader)header;

- (BOOL)isSubHandleForHeader:(BlockHeader)header extra:(unsigned)extra atIndex:(unsigned)index;

- (NSString *)descriptionForHeader:(BlockHeader)header extra:(unsigned)extra;

@end

/***************	Common root store class	***************/

@interface _PPLStoreRoot:NSObject { // really exists for implementation purposes
    NSMutableData	*current;	/* current image of 'everything' */
    unsigned		quadLocation;
    unsigned		quadUnsigned[4];
}

- (unsigned)rootIndex;

- (unsigned)totalRoundedLengthOfBlock:(unsigned)handle;
    /* Including header; this knows a little about the schema */

- (BlockHeader)readHeaderInBlock:(unsigned)handle;
    /* Reads the header! 
    May cause following blocks to be read in oder to fill the free / list and compact */

- (unsigned)unsignedForBlock:(unsigned)handle atIndex:(unsigned)index;

- (void)readDataInBlock:(unsigned)handle intoData:(NSMutableData *)mdata;

- readPropertyListInBlock:(unsigned)handle;

- (NSString *)readNEXTSTEPStringInBlock:(unsigned)handle notifyCache:(BOOL)cache;

- (void)initString:(_PPLString *)string inBlock:(unsigned)handle;

- (void)autoCompactForHeader:(BlockHeader)header inBlock:(unsigned)handle;
    // for incremental per-page compaction;  defined to do nothing in common class
    
@end

/***************	Store class	***************/

@interface _PPLStore:_PPLStoreRoot {
    NSString		*path;
    NSFileHandle	*datafd;	
    				/* remains open at all times */
				/* Set to nil when a fatal error occured */
    unsigned long	lastLengthSyncedData;
    				/* length of data file when last synced */
    unsigned		lengthToBeAppended;
    BOOL		dataNeedsSync;
    BOOL		logNeedsTruncate;
    BOOL		readOnly;
    BOOL		invalid; /* Set to YES when a fatal error occured */
    unsigned		version;
    id <PPLTypeProvider>	typeProvider;
    id			logInfo;
    unsigned		numCleanRanges;
    NSRange		*cleanRanges;	/* sorted, no max (because slow to insert anyway) */
    unsigned		hintIndex;
    id			freeInfo;
}

+ (int)versionForStoreAtPath:(NSString *)path writable:(BOOL *)writable;
    /* returns store version if a valid store there;
    returns -1 if file does not exist;
    returns -2 if directory wrapper turns out to be a file;
    returns -3 if some element is not readable;
    returns -4 if not the proper magic;
    also returns if the store is writable (from the FS point of vue) */

+ (int)versionForStoreAtPath:(NSString *)path withPPLData:(NSData *)pplData;
    /* returns store version if a valid store there;
    returns -6 if file can't be created;
    returns -5 if some file already exists;
    returns -4 if not the proper magic;
    also returns if the store is writable (from the FS point of vue) */

- initWithNewStorePath:(NSString *)path typeProvider:(id <PPLTypeProvider>)typeProvider;

- initWithExistingStoreAtPath:(NSString *)path readOnly:(BOOL)readOnly typeProvider:(id <PPLTypeProvider>)typeProvider;

- initWithNewStorePath:(NSString *)path fromPPLData:(NSData *)pplData readOnly:(BOOL)readOnly typeProvider:(id <PPLTypeProvider>)typeProvider;

- (NSString *)path;

/***************	Block creation / modification	***************/

- (unsigned)createBlockForHeader:(BlockHeader)header andData:(NSData *)data reuseFreeBlock:(BOOL)reuse cold:(BOOL)cold trueCapacity:(unsigned *)trueCapacity;
    /* if trueCapacity is non-NULL returns the true capacity (excluding header);
    iff reuse, tries to reuse the free list; in that case cold indicates whether to pick a cold handle or a handle at random */

- (void)writeHeader:(BlockHeader)header inBlock:(unsigned)handle;

- (void)writeUnsigned:(unsigned)val inBlock:(unsigned)handle atIndex:(unsigned)index;

- (void)downShiftUnsigned:(NSRange)range inBlock:(unsigned)handle by:(unsigned)shift;
    // shifts << several unsigned

/***************	Freeing blocks	***************/

- (void)freeBlock:(unsigned)handle;

- (void)mergeFreeBlockWithNext:(unsigned)handle;

- (unsigned)splitFreeBlockInTwo:(unsigned)handle atIndex:(unsigned)firstBlockLength;

/***************	Control		***************/

- (void)reflectCurrentOnDisk;

- (void)tryReflectCurrentOnDiskAndRemap;
    /* estimates the size of the divergence between the in memory and the on-disk representation;
    If it is of the order of the amount of real memory, causes a reflectCurrentOnDisk followed by a remap */
    
- (void)reallySyncPart1;
    // if we never return from this method, the save is aborted

- (void)reallySyncPart2;
    // Whether or not we return from this method, the save has succeeded

- (BOOL)reloadFreeListUntilAbortPredicate:(BOOL (*)(void *))abortion andContext:(void *)context;
    /* returns YES if aborted (predicate returned YES), NO otherwise */

- (NSData *)contentsAsData;
    /* Returns a data expressing the entire store */

/***************	Misc		***************/

- (void)_rawDumpFile;

@end

/***************	Store as PPL Data		***************/

@interface _PPLReadOnlyStore:_PPLStoreRoot

- initWithPPLData:(NSData *)pplData;
    // retains pplData

@end


/***************	Definitions	***************/

// Magic constants
#define CURRENT_VERSION 3

@interface PPL (ForwardReference)
- (void)_setDirty;
@end

/***************	Utilities	***************/

void _PPLIncrementInternalRefCount(id obj, unsigned *countVar, unsigned mask, NSLock *lock) {
    if (lock) [lock lock];
    if ((*countVar & mask) != mask) (*countVar)++;
    else NSIncrementExtraRefCount(obj);
    if (lock) [lock unlock];
}

BOOL _PPLDecrementInternalRefCountWasZero(id obj, unsigned *countVar, unsigned mask, NSLock *lock) {
    BOOL result = NO;
    if (lock) [lock lock];
    if (0 == (*countVar & mask)) result = YES;
    else if ((*countVar & mask) != mask) (*countVar)--;
    else if (NSDecrementExtraRefCountWasZero(obj)) (*countVar)--;
    if (lock) [lock unlock];
    return result;
}

unsigned _PPLInternalRefCount(id obj, unsigned countVar, unsigned mask) {
    unsigned int count;
    if ((countVar & mask) != mask) count = (countVar & mask) + 1;
    else {
        unsigned long long tmp = mask + NSExtraRefCount(obj) + 1;
        count = (UINT_MAX < tmp) ? UINT_MAX : (unsigned int)tmp;
    }
    return count;
}

@implementation PPL

static BOOL flushPPLCache = NO;

static BOOL logPPLCreationAndDestruction = NO;
static BOOL bcopyForPPLAsData = NO;

+ (void)_setCacheFlushing:(BOOL)flag {
    flushPPLCache = flag;
}

- (_PPLCache *)_cache {
    return cache;
}

- (_PPLStore *)_store {
    return store;
}

- (void)setCacheHalfLife:(NSTimeInterval)halfLife {
    if (halfLife < 0) {
	[cache release];
	cache = nil;
    } else {
	[cache setHalfLife:halfLife];
    }
}

- (void)_raiseFormat:(NSString *)format, ... {
    va_list		args;
    NSString		*reason;
    NSDictionary	*userInfo = [NSDictionary dictionaryWithObjectsAndKeys:self, @"PPL", 0];
    va_start(args, format); 
    reason = [[[NSString allocWithZone:NULL] initWithFormat:format arguments:args] autorelease];
    [[NSException exceptionWithName:NSInternalInconsistencyException reason:reason userInfo:userInfo] raise];
    va_end(args);
}

static void pplBug(PPL *self, SEL _cmd) {
    [self _raiseFormat:@"*** PPL exception in %@", NSStringFromSelector(_cmd)];
}

/***************	Primitives	***************/

- (unsigned)_freezeImmutableObject:object {
    /* format: (refCount, type, lengthOfSerialization - 4, serialization) */
    NSMutableData	*mdata = [[NSMutableData allocWithZone:NULL] init];
    BlockHeader	header = {1, STORED_IOBJECT, 0};
    unsigned		handle;
    [NSSerializer serializePropertyList:object intoData:mdata];
    header.capacity = [mdata length];
    handle = [store createBlockForHeader:header andData:mdata reuseFreeBlock:YES cold:YES trueCapacity:NULL];
    [mdata release];
    return handle;
}

- (void)_incrRefCountInStoreFor:(unsigned)handle {
    BlockHeader	header = [store readHeaderInBlock:handle];
    if (header.refCount == LARGEST_REFCOUNT) return;
    header.refCount++;
    [store writeHeader:header inBlock:handle];
}

- (void)_reallyRecycleStorageForHandle:(unsigned)handle {
    BlockHeader	header = [store readHeaderInBlock:handle];
    if (header.refCount) [self _raiseFormat:@"*** %@ : %d", NSStringFromSelector(_cmd), handle];
    header.type = STORED_FREE;
    [store writeHeader:header inBlock:handle];
    [store freeBlock:handle];
}

- (void)_decrRefCountInStoreFor:(unsigned)handle {
    BlockHeader	header = [store readHeaderInBlock:handle];
    if (header.refCount == LARGEST_REFCOUNT) return;
    if (!header.refCount) [self _raiseFormat:@"*** %@ null refCount for ^%x", NSStringFromSelector (_cmd), handle];
    header.refCount--;
    [store writeHeader:header inBlock:handle];
    if (header.refCount) return;
    switch (header.type) {
	case STORED_NSSTRING:
	case STORED_IOBJECT:
	    [self _reallyRecycleStorageForHandle:handle];
	    [cache removeCachedObjectForHandle:handle];
	    break;
	case STORED_FORWARD: {
	    unsigned	newHandle = [store unsignedForBlock:handle atIndex:0];
	    id		mobject = [mutables mutableObjectForHandle:handle];
	    if (mobject) {
		[mobject retain]; // attention: must be done first
		[mutables forgetMutableObjectHandle:handle]; // does a release
		[mutables noteHandle:newHandle forMutableObject:mobject];
		[mobject release];
	    }
	    [self _decrRefCountInStoreFor:newHandle];
	    [mutables forgetMutableHandleToDestroy:handle];
	    [self _reallyRecycleStorageForHandle:handle];
	    break;
	}
	case STORED_MARRAY: 
	case STORED_MDICT:
	    /* We delay the destruction, because a further retain could bring this handle back to life */
	    [mutables noteMutableHandleToDestroy:handle];
	    break;
	default:
	    pplBug(self, _cmd);
    }
}

- (void)_decrRefCountInStoreMultiple:(PPLOrderedHeap *)handles {
    unsigned	sub;
    while ((sub = (unsigned)PPLRemoveMinimumOfOrderedHeap(handles))) {
	[self _decrRefCountInStoreFor:sub];
    }
}

- (unsigned)_noCreateHandleForValue:value {
    unsigned	handle = [cache cachedHandleForObject:value];
    if (handle) return handle;
    return [mutables handleForMutableObject:value];
}

- (unsigned)_store:value {
    unsigned	handle = [cache cachedHandleForObject:value];
    if (!handle) handle = [mutables handleForMutableObject:value];
    if (handle) {
	/* instead of storing a new one to that same reference we just increment the reference count */
	[self _incrRefCountInStoreFor:handle];
	if ([mutables forgetMutableHandleToDestroy:handle]) {
	    /* we are bringing back a mutable handle from the dead */
	    //PPLLog(@"=== Storing %@ again; in-store is back to 1", value);
	}
	return handle;
    } else if ([value conformsToProtocol:@protocol(_PPLMutableObject)] && ([value _ppl] == self)) {
	/* we do a double check because we could have a stored object from a different PPL */
	handle = [value _handle];
	/* instead of storing a new one, we just increment the reference count */
	[self _incrRefCountInStoreFor:handle];
	if ([mutables forgetMutableHandleToDestroy:handle]) {
	    /* we are bringing back a mutable handle from the dead */
	    //PPLLog(@"=== Storing %@ again; in-store is back to 1", value);
	}
	return handle;
    } else if ([value isKindOfClass:[NSData class]]) {
	return [self _freezeImmutableObject:value];
    } else if ([value isKindOfClass:[NSMutableArray class]]) {
	NSMutableArray	*marray = value;
	unsigned	count = [marray count];
	handle = [_PPLMutableArray createArrayBlockInStore:store max:&count];
	[mutables noteHandle:handle forMutableObject:marray];
	return handle;
    } else if ([value isKindOfClass:[NSMutableDictionary class]]) {
	NSMutableDictionary	*mdict = value;
	unsigned		count = [mdict count];
	handle = [_PPLMutableDict createDictBlockInStore:store capacity:count];
	[mutables noteHandle:handle forMutableObject:mdict];
	return handle;
    } else if ([value isKindOfClass:[NSArray class]] || [value isKindOfClass:[NSDictionary class]] || [value isKindOfClass:[NSString class]]) {
	return [self _freezeImmutableObject:value];
    } else {
	PPLLog(@"Can't store value: %@", value);
    }
    return 0;
}

- (void)_destroyMutableHandle:(unsigned)handle {
    /* note that this may add more objects in mutables */
    BlockHeader	header = [store readHeaderInBlock:handle];
    /* no other block points to this block anymore! */
    if (header.refCount) pplBug(self, _cmd);
    switch (header.type) {
	case STORED_FORWARD: {
	    unsigned	newHandle = [store unsignedForBlock:handle atIndex:0];
	    [self _decrRefCountInStoreFor:newHandle];
	    break;
	}
	case STORED_MARRAY: 
	case STORED_MDICT: {
	    id	object = _PPLRetrieve(self, handle, NO);
	    [object _destroySubsInStorage];
	    break;
	}
	default:
	    pplBug(self, _cmd);
    }
    [self _reallyRecycleStorageForHandle:handle];
}

- (BOOL)_forgetHandleToObjectAndDestroyIfRefCountIsOne:(unsigned)mhandle {
    /* returns YES if it did something ... */
    id		mobject = [mutables mutableObjectForHandle:mhandle];
    BOOL	doneSomething = NO;
    if (!mobject || ([mobject retainCount] == 1)) {
	/* We can destroy it! */
	if ([mutables forgetMutableObjectHandle:mhandle]) doneSomething = YES;
	if ([mutables forgetMutableHandleToDestroy:mhandle]) {
	    [self _destroyMutableHandle:mhandle];
	    doneSomething = YES;
	}
    }
    return doneSomething;
}

- (BOOL)_forgetAndDestroyMultiple:(unsigned *)mhandles count:(unsigned)count {
    BOOL	flushedOneAtLeast = NO;
    while (count--) {
	unsigned	mhandle = mhandles[count];
	if ([self _forgetHandleToObjectAndDestroyIfRefCountIsOne:mhandle]) {
	    flushedOneAtLeast = YES;
	}
    }
    return flushedOneAtLeast;
}

- (void)_freezeMutableObjectsAndFlushRefcountOne {
    unsigned	count;
    unsigned	*handles;
    BOOL	frozeOneAtLeast = YES;
    BOOL	flushedOneAtLeast = YES;
    NSHashTable	*visited = NSCreateHashTableWithZone(NSNonOwnedPointerHashCallBacks, 0, NULL);
    while (frozeOneAtLeast) {
	frozeOneAtLeast = NO;
	handles = NULL; // in case !mutables 
	count = [mutables getHandlesToMutableObjectsInto:&handles];
	while (count--) {
	    unsigned	handle = handles[count];
	    if (!NSHashGet(visited, (void *)handle)) {
		id	mobject = [mutables mutableObjectForHandle:handle];
		[mobject _freezeForHandle:handle inPPL:self];
		NSHashInsert(visited, (void *)handle);
		frozeOneAtLeast = YES;
	    }
	}
	NSZoneFree(NSZoneFromPointer(handles), handles);
    }
    /* visited is now all the mutable objects handles */
    NSFreeHashTable(visited);
    while (flushedOneAtLeast) {
	flushedOneAtLeast = NO;
	/* We now empty handleToMObject of all the objects with a ref count of 1 */
	handles = NULL; // in case !mutables 
	count = [mutables getHandlesToMutableObjectsInto:&handles];
	if (count) {
	    if ([self _forgetAndDestroyMultiple:handles count:count]) {
		flushedOneAtLeast = YES;
	    }
	    NSZoneFree(NSZoneFromPointer(handles), handles);
	}
	/* We now destroy the handles to destroy that are not retained */
	handles = NULL; // in case !mutables 
	count = [mutables getHandlesToDestroyInto:&handles];
	if (count) {
	    if ([self _forgetAndDestroyMultiple:handles count:count]) {
		flushedOneAtLeast = YES;
	    }
	    NSZoneFree(NSZoneFromPointer(handles), handles);
	}
    }
}

static void compactForwardingHandle(PPL *self, unsigned origin, unsigned idx, unsigned handle, unsigned newHandle) {
    //PPLLog(@"Compacting handle %d -> %d", handle, newHandle);
    if (!newHandle) [self _raiseFormat:@"*** PPL: compactForwardingHandle()"];
    [self _setDirty];
    [self->store writeUnsigned:newHandle inBlock:origin atIndex:idx];
    [self _incrRefCountInStoreFor:newHandle];
    [self _decrRefCountInStoreFor:handle];
}

id _PPLRetrieveCompact(PPL *self, unsigned origin, unsigned idx, BOOL flag) {
    unsigned	handle = [self->store unsignedForBlock:origin atIndex:idx];
    if (!self->readOnly) {
	NS_DURING
	    if ([self->store readHeaderInBlock:handle].type == STORED_FORWARD) {
		unsigned	newHandle = [self->store unsignedForBlock:handle atIndex:0];
		id		object;
		compactForwardingHandle(self, origin, idx, handle, newHandle);
		object = _PPLRetrieveCompact(self, origin, idx, flag);
		NS_VALUERETURN(object, id);
	    }
	NS_HANDLER
	    [self _raiseFormat:@"***_PPLRetrieveCompact: Damaged PPL"];
	NS_ENDHANDLER
    }
    return _PPLRetrieve(self, handle, flag);
}

static unsigned followForwardingIndirectAndWrite(PPL *self, unsigned origin, unsigned idx) {
    unsigned	handle = 0;
    NS_DURING
	handle = [self->store unsignedForBlock:origin atIndex:idx];
	if ([self->store readHeaderInBlock:handle].type == STORED_FORWARD) {
	    unsigned	newHandle = [self->store unsignedForBlock:handle atIndex:0];
	    compactForwardingHandle(self, origin, idx, handle, newHandle);
	    handle = followForwardingIndirectAndWrite(self, origin, idx);
	}
    NS_HANDLER
	[self _raiseFormat:@"*** followForwardingIndirectAndWrite: Damaged PPL"];
    NS_ENDHANDLER
    return handle;
}

unsigned _PPLFollowForwardingHandles(PPL *self, unsigned handle) {
    NS_DURING
	if ([self->store readHeaderInBlock:handle].type != STORED_FORWARD) NS_VALUERETURN(handle, unsigned);
	if (!self->readOnly) NS_VALUERETURN(followForwardingIndirectAndWrite(self, handle, 0), unsigned);
	NS_VALUERETURN(_PPLFollowForwardingHandles(self, [self->store unsignedForBlock:handle atIndex:0]), unsigned);
    NS_HANDLER
	[self _raiseFormat:@"*** _PPLFollowForwardingHandles: Damaged PPL"];
    NS_ENDHANDLER
    return 0;
}

id _PPLRetrieve(PPL *self, unsigned handle, BOOL flag) {
    id		value;
    unsigned	type = 0;
    value = [self->cache cachedObjectForHandle:handle];
    if (value) return value;
    value = [self->mutables mutableObjectForHandle:handle];
    if (value) {
	if ([value retainCount] == 1 && !([value conformsToProtocol:@protocol(_PPLMutableObject)] && ([value _ppl] == self))) {
	    [value _freezeForHandle:handle inPPL:self];
	    [self->mutables forgetMutableObjectHandle:handle];
	    value = nil;
	} else {
	    return value;
	}
    }
    NS_DURING
	type = [self->store readHeaderInBlock:handle].type;
    NS_HANDLER
	[self _raiseFormat:@"*** _PPLRetrieve: Damaged PPL"];
    NS_ENDHANDLER
    switch (type) {
	case STORED_NSSTRING:
	    if (flag) {
                value = [_PPLString allocWithZone:NULL];
		[self->store initString:value inBlock:handle]; 
		[self->cache addCachedObject:value handle:handle];
		return [value autorelease];
	    } else {
		return [self->store readNEXTSTEPStringInBlock:handle notifyCache:YES];
	    }
	case STORED_IOBJECT:
	    if (flag) {
		id pool = [NSAutoreleasePool new];
		id	plist = [self->store readPropertyListInBlock:handle];
		value = [[_PPLPropertyList allocWithZone:NULL] initWithRealPropertyList:plist];
                [pool release];
		[self->cache addCachedObject:value handle:handle];
		return [value autorelease];
	    } else {
		return [self->store readPropertyListInBlock:handle];
	    }
	case STORED_MARRAY:
	    value = [[_PPLMutableArray allocWithZone:NULL] initWithPPL:self forHandle:handle]; 
	    [self->mutables noteHandle:handle forMutableObjectNonRetained:value];
	    return value;
	case STORED_MDICT:
	    value = [[_PPLMutableDict allocWithZone:NULL] initWithPPL:self forHandle:handle];
	    [self->mutables noteHandle:handle forMutableObjectNonRetained:value];
	    return value;
	case STORED_FORWARD:
	    //PPLLog(@"Going through forwarding handle %d", handle);
	    return _PPLRetrieveCompact(self, handle, 0, flag);
	default:
	    [self _raiseFormat:@"*** _PPLRetrieve() Damaged PPL (unknown ref code: %d)", type];
	    return nil;
    }
}

- (void)flushCache {
    unsigned	count;
    id		*objects;
    unsigned	*handles;
    count = [cache count];
    objects = NSZoneMalloc(NULL, count * sizeof(id));
    handles = NSZoneMalloc(NULL, count * sizeof(unsigned));
    [cache getAllCachedObjectsInto:objects handles:handles max:count];
    while (count--) {
	id		object = objects[count];
	unsigned	handle = handles[count];
	BlockHeader	header = [store readHeaderInBlock:handle];
	/* we check that the ref count on disk is at least 1 */
	if (!header.refCount) {
	    PPLLog(@"*** Disk leak for object %@", object);
	}
    }
    NSZoneFree(NSZoneFromPointer(objects), objects);
    NSZoneFree(NSZoneFromPointer(handles), handles);
    [cache removeAllCachedObjects];
}

/***************	PPL basics	***************/

+ (PPL *)pplWithPath:(NSString *)path create:(BOOL)create readOnly:(BOOL)ro {
    return [[[self allocWithZone:NULL] initWithPath:path create:create readOnly:ro] autorelease];
}

- initWithPath:(NSString *)path create:(BOOL)create readOnly:(BOOL)ro {
    BOOL	writable;
    int		vers = [_PPLStore versionForStoreAtPath:path writable:&writable];
    readOnly = ro;
    switch (vers) {
	case -1:
		if (!create || readOnly) {
		    PPLLog(@"*** Can't read PPL store at %@", path);
		    goto nope;
		}
		store = [[_PPLStore allocWithZone:NULL] initWithNewStorePath:path typeProvider:[self class]];
		version = CURRENT_VERSION;
		rootHandle = [_PPLMutableDict createDictBlockInStore:store capacity:0];
		[store writeUnsigned:rootHandle inBlock:0 atIndex:[store rootIndex]];
		[store reallySyncPart1];
		[store reallySyncPart2];
		break;
	case -2:
		PPLLog(@"*** %@ is not a PPL store: expecting directory", path);
		goto nope;
	case -3:
		PPLLog(@"*** Can't read PPL store at %@", path);
		goto nope;
	case -4:
		PPLLog(@"*** Incorrect magic number for PPL store %@", path);
		goto nope;
	default:
		if (!writable && !readOnly) {
		    PPLLog(@"*** PPL store at %@ can not be written; store only opened for reading", path);
		    readOnly = YES;
		}
		if (!readOnly && (vers != CURRENT_VERSION)) {
		    PPLLog(@"*** Changes to version %d files not allowed; PPL store only opened for reading", vers);
		    readOnly = YES;
		}
		store = [[_PPLStore allocWithZone:NULL] initWithExistingStoreAtPath:path readOnly:readOnly typeProvider:[self class]];
		version = vers;
		rootHandle = [store unsignedForBlock:0 atIndex:[store rootIndex]];
		break;
    }
    if (!store) goto nope;
    mutables = [[_PPLMutables allocWithZone:NULL] init];
    cache = [[_PPLCache allocWithZone:NULL] init];
    if (logPPLCreationAndDestruction) PPLLog(@"Created PPL at %@", path);
    return self;
  nope:
    [self dealloc]; 
    return nil;
}

- (void)detachFromFile {
    unsigned	count;
    unsigned	*handles;
    /* We may have proxies waiting in the auto-release pools;
    Mutable objects that are not PPL proxies we don't care; 
    The PPL proxies we zombify so that we can release them */
    handles = NULL; // in case !mutables 
    count = [mutables getHandlesToMutableObjectsInto:&handles];
    while (count--) {
	unsigned	handle = handles[count];
	id		mobject = [mutables mutableObjectForHandle:handle];
	unsigned	retainCount = [mobject retainCount];
	[mutables forgetMutableObjectHandle:handle];
	if ((retainCount > 1) && [mobject conformsToProtocol:@protocol(_PPLMutableObject)] && ([mobject _ppl] == self)) {
	    //PPLLog(@"Zombified ^%x 0x%x", handle, mobject);
	    [mobject _zombifyInPPL:self];
	}
    }
    NSZoneFree(NSZoneFromPointer(handles), handles);
    [mutables release];
    mutables = nil;
    [self flushCache];
    [cache release];
    cache = nil;
    if (logPPLCreationAndDestruction) PPLLog(@"Detaching PPL from %@", [store path]);
    [store release];
    store = nil;
}

- (void)dealloc {
    [self detachFromFile];
    [super dealloc];
}

- (NSMutableDictionary *)rootDictionary {
    if (version == CURRENT_VERSION) {
	return _PPLRetrieveCompact(self, 0, [store rootIndex], YES);
    } else {
	pplBug(self, _cmd);
	return nil;
    }
}

- (NSString *)description {
    return [(id)([self rootDictionary]) description];
}

/*****		Generating Data expressing the entire PPL		*****/

- (NSData *)contentsAsData {
    NSData	*data;
    if (!readOnly && (isDirty || [mutables _hasNonProxyMutable])) {
	[self _freezeMutableObjectsAndFlushRefcountOne];
    }
    data = [store contentsAsData];
    if (bcopyForPPLAsData) {
	unsigned	length = [data length];
	void		*bytes = NSZoneMalloc(NULL, length);
	[data getBytes:bytes];
	data = [NSData dataWithBytesNoCopy:bytes length:length];
    }
    return data;
}

+ (PPL *)pplWithPath:(NSString *)path fromPPLData:(NSData *)pplData readOnly:(BOOL)ro {
    return [[[self allocWithZone:NULL] initWithPath:path fromPPLData:pplData readOnly:ro] autorelease];
}

- initWithPath:(NSString *)path fromPPLData:(NSData *)pplData readOnly:(BOOL)ro {
    int		vers = [_PPLStore versionForStoreAtPath:path withPPLData:pplData];
    readOnly = ro;
    switch (vers) {
	case -6:
		PPLLog(@"*** PPL store can't be created at %@", path);
		goto nope;
	case -5:
		PPLLog(@"*** File already exists; can't create PPL store at %@", path);
		goto nope;
	case -4:
		PPLLog(@"*** Incorrect magic for PPL store at %@", path);
		goto nope;
	default:
		if (!readOnly && (vers != CURRENT_VERSION)) {
		    PPLLog(@"*** Changes to version %d files not allowed; PPL store only opened for reading", vers);
		    readOnly = YES;
		}
		store = [[_PPLStore allocWithZone:NULL] initWithNewStorePath:path fromPPLData:pplData readOnly:readOnly typeProvider:[self class]];
		version = vers;
		rootHandle = [store unsignedForBlock:0 atIndex:[store rootIndex]];
		break;
    }
    if (!store) goto nope;
    mutables = [[_PPLMutables allocWithZone:NULL] init];
    cache = [[_PPLCache allocWithZone:NULL] init];
    if (logPPLCreationAndDestruction) PPLLog(@"Created PPL at %@", path);
    return self;
  nope:
    [self dealloc]; 
    return nil;
}

+ (NSDictionary *)propertyListWithPPLData:(NSData *)pplData {
    _PPLReadOnlyStore		*dstore = [[_PPLReadOnlyStore allocWithZone:NULL] initWithPPLData:pplData];
    unsigned			handle = [dstore unsignedForBlock:0 atIndex:[dstore rootIndex]];
    NSDictionary		*dict = [_PPLDataDictionary retrieveRootDataStore:dstore forHandle:handle];
    [dstore release];
    return dict;
}

/***************	Type checking 	***************/

+ (BOOL)checkValidHeader:(BlockHeader)header {
    switch (header.type) {
	case STORED_FREE:
		if (header.refCount) return NO;
		return YES;
	case STORED_MARRAY:
	case STORED_MDICT:
	case STORED_NSSTRING:
	case STORED_IOBJECT:
	case STORED_FORWARD:
		if (!header.refCount) PPLLog(@"Disk leak of size: %d", header.capacity);
		return YES;
	default:
		return NO;
    }
}

+ (BOOL)isSubHandleForHeader:(BlockHeader)header extra:(unsigned)extra atIndex:(unsigned)idx {
    switch (header.type) {
	case STORED_MARRAY:
		/* extra is the array count */
		if (!idx) return NO; 
		if (idx - 1 >= extra) return NO;
		return YES;
	case STORED_MDICT:
		/* extra is the dictionary count */
		if (!idx) return NO; 
		if (!((idx - 1) % 3)) return NO;
		return YES;
	case STORED_FORWARD:
		if (!idx) return YES; 
		return NO;
	default:
		return NO;
    }
}

+ (NSString *)descriptionForHeader:(BlockHeader)header extra:(unsigned)extra {
    NSString	*name;
    BOOL	isHandle = NO;
    BOOL	isCount = NO;
    switch (header.type) {
	case STORED_FREE:	
		name = @"FREE"; break;
	case STORED_MARRAY:
		name = @"MARRAY"; isCount = YES; break;
	case STORED_MDICT:
		name = @"MDICT"; isCount = YES; break;
	case STORED_NSSTRING:
		name = @"NSSTRING"; break;
	case STORED_IOBJECT:
		name = @"IOBJECT"; break;
	case STORED_FORWARD:
		name = @"FORWARD"; isHandle = YES; break;
	default:
		name = [NSString stringWithFormat:@"*** Unknown type 0x%x", header.type]; break;
    }
    if (!isHandle && isCount)
	return [NSString stringWithFormat:@"%@ (%d refs, %d capa, %d count)", name, header.refCount, header.capacity, extra];
    return [NSString stringWithFormat:@"%@ (%d refs, %d capa)", name, header.refCount, header.capacity];
}

/***************	Transactions 	***************/

- (void)_setDirty {
    if (isDirty) return;
    if (readOnly) [NSException raise:NSInvalidArgumentException format:@"*** Can not mutate read only store"];
    [[NSNotificationCenter defaultCenter] postNotificationName:PPLDidBecomeDirtyNotification object:self userInfo:nil];
    isDirty = YES;
}

static BOOL copyAfterSave = NO;

+ (void)_setCopyAfterSave:(BOOL)enable {
    copyAfterSave = enable;
}

static id lastCopyAfterSave = nil;

+ (id)_lastCopyAfterSave {
    return lastCopyAfterSave;
}

- (void)save {
    id	copyJustInCase = nil;
    if (readOnly) {
	PPLLog(@"*** PPL changes not allowed: read-only");
	return;
    }
    if (copyAfterSave) copyJustInCase = [[self rootDictionary] copyWithZone:NULL];
    /* It is tempting to check for the 'isDirty' flag only, however in the case of non-proxy mutable objects, the mutable object may have been modified, without the 'isDirty' flag ever modified */
    if (isDirty || [mutables _hasNonProxyMutable]) {
	[self _freezeMutableObjectsAndFlushRefcountOne];
	if (flushPPLCache) [self flushCache];
	isDirty = NO;
    }
    NS_DURING
	[store reallySyncPart1];
    NS_HANDLER
	/* reallySyncPart1 failed, the PPL is logically NOT saved */
	[copyJustInCase release];
	[localException raise];
    NS_ENDHANDLER
    // We can't do anything on the PPL here
    NS_DURING
	[store reallySyncPart2];
    NS_HANDLER
	/* Even though reallySyncPart2 failed, the PPL is logically saved */
	if (copyAfterSave) {
	    [lastCopyAfterSave autorelease];
	    lastCopyAfterSave = copyJustInCase;
	}
	[localException raise];
    NS_ENDHANDLER
    if (copyAfterSave) {
	[lastCopyAfterSave autorelease];
	lastCopyAfterSave = copyJustInCase;
    }
    [[NSNotificationCenter defaultCenter] postNotificationName:PPLDidSaveNotification object:self userInfo:nil];
}

- (void)flush {
    if (readOnly) {
	PPLLog(@"*** PPL changes not allowed: read-only");
	return;
    }
    if (!isDirty && ![mutables _hasNonProxyMutable]) return;
    [self _freezeMutableObjectsAndFlushRefcountOne];
    if (flushPPLCache) [self flushCache];
    [store reflectCurrentOnDisk];
    isDirty = NO;
}

- (void)pushChangesToDisk {
    if (readOnly) {
	PPLLog(@"*** PPL changes not allowed: read-only");
	return;
    }
    if (!isDirty && ![mutables _hasNonProxyMutable]) return;
    [store reflectCurrentOnDisk];
    // we do not reset isDirty because there may be objects to freeze
}

- (BOOL)reloadFreeListUntilAbortPredicate:(BOOL (*)(void *))abortion andContext:(void *)context {
    BOOL	aborted;
    if (readOnly) {
	PPLLog(@"*** reloadFreeListUntilDate: not allowed on read-only PPL");
	return YES;
    }
    [self flush];
    aborted = [store reloadFreeListUntilAbortPredicate:abortion andContext:context];
    [store reflectCurrentOnDisk]; // we write the changes
    return aborted;
}

/***************	Special Dump 	***************/

static void printSpaces(NSMutableString *out, unsigned indent) {
    while (indent >= 2) {
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"\t"];
	indent -= 2;
    }
    if (indent) [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"    "];
}

static void printRefCount(NSMutableString *out, id root) {
    unsigned	refCount;
    /* Attention: printed refCount is only a hint because there can be carried over refCount from forward blocks */
    if ([root respondsToSelector:@selector(_inStoreRefCount)]) {
	refCount = [root _inStoreRefCount];
    } else {
	refCount = [root retainCount];
    }
    if (refCount > 1) {
	NSString	*temp = [[NSString allocWithZone:NULL] initWithFormat:@"/* %d refs */ ", refCount];
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:temp];
	[temp release];
    }
}

static BOOL dump(NSMutableString *out, id root, unsigned indent) {
    BOOL ok = YES;
    NS_DURING
    if (!root) {
	PPLLog(@"*** Encountered nil");
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"ERROR"]; // in order to generate a legal ASCII decompiling
    } else if ([root isKindOfClass:[NSString class]]) {
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:root];
    } else if ([root isKindOfClass:[NSArray class]]) {
	NSArray 	*array = root;
	unsigned	count;
	unsigned	idx = 0;
	id pool = [NSAutoreleasePool new];
	if (![root isKindOfClass:[NSMutableArray class]]) [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"/*IMM*/ "];
	printRefCount(out, array);
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"("];
	count = [array count];
	while (idx < count) {
	    NS_DURING
		if (idx) [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@", "];
		if (count > 40) {
		    [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"\n"];
		    printSpaces(out, indent);
		}
		if (!dump(out, [array objectAtIndex:idx], indent + 1)) ok = NO;
	    NS_HANDLER 
		PPLLog(@"*** Raised during List enumeration for index %d out of %d", idx, count); 
	     NS_ENDHANDLER
	    idx++;
	}
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@")"];
        [pool release];
    } else if ([root isKindOfClass:[NSDictionary class]]) {
	NSDictionary 		*dict = root;
	NSArray			*allKeys = nil;
	unsigned		count;
	unsigned		idx = 0;
        id pool = [NSAutoreleasePool new];
	NS_DURING
	    allKeys = [dict allKeys];
	NS_HANDLER 
	    PPLLog(@"*** Raised during PropertyList enumeration"); 
	 NS_ENDHANDLER
	/* We sort so that we can diff the output */
	allKeys = [allKeys sortedArrayUsingSelector:@selector(compare:)];
	count = [allKeys count];
	if (![root isKindOfClass:[NSMutableDictionary class]]) [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"/*IMM*/ "];
	printRefCount(out, dict);
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"{\n"];
	while (idx < count) {
	    NSString 	*key = [allKeys objectAtIndex:idx];
	    NS_DURING
		id	value = [root objectForKey:key];
		if (value) {
		    printSpaces(out, indent);
		    if (!dump(out, key, 0)) ok = NO;
		    [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@" = "];
		    if (!dump(out, value, indent+1)) ok = NO;
		    [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@";\n"];
		}
	    NS_HANDLER 
		PPLLog(@"*** Raised during PropertyList objectForKey:"); 
	     NS_ENDHANDLER
	    idx++;
	}
	if (indent) printSpaces(out, indent-1);
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"}"];
        [pool release];
    } else {
	[out replaceCharactersInRange:NSMakeRange([out length], 0) withString:[root description]];
    }
    NS_HANDLER 
	PPLLog(@"*** Raised");
	ok = NO; 
     NS_ENDHANDLER
    return ok;
}

- (NSString *)_specialDump {
    NSMutableString	*out = [[NSMutableString allocWithZone:NULL] init];
    if (!dump(out, [self rootDictionary], 1)) {
	PPLLog(@"*** Damaged tree: result may not be a valid property list ***");
    }
    [out replaceCharactersInRange:NSMakeRange([out length], 0) withString:@"\n"];
    return [out autorelease];
}

+ (void)_setTestRaiseCount:(unsigned)counter {
    _PPLRaiseCount = counter;
    PPLLog(@"Setting raiseCount to %d", _PPLRaiseCount);
}

@end

#define MIN_CACHE_MAX		7	// must be 2**N - 1
#define REHASH_THRESHOLD	50 	// in percent

static BOOL _PPLCacheLog = NO;

@implementation _PPLCache

static NSHashTable *allCaches = NULL;
static NSLock *_PPLAllCachesLock = nil;

+ (void)initialize {
    // Could be done lazily
    _PPLAllCachesLock = [[NSLock allocWithZone:NULL] init];
}

static _PPLCacheEntry *deleted = NULL;

static void addEntry(_PPLCache *self, _PPLCacheEntry *entry);
    /* refreshes the entry date at the same time */

static void addEntries(_PPLCache *self, _PPLCacheEntry **entries, unsigned num) {
    while (num--) {
	_PPLCacheEntry	*entry = *(entries++);
	if (entry && (entry != deleted)) addEntry(self, entry);
    }
}

static unsigned HHASH(unsigned handle) {
    unsigned	usefulBits = handle >> 4;
    unsigned	xored = (usefulBits & 0xffff) ^ (usefulBits >> 16);
    return (xored * 65521) + usefulBits;
}

static unsigned OHASH(id object) {
    unsigned	usefulBits = ((unsigned)object) >> 3;
    unsigned	xored = (usefulBits & 0xffff) ^ (usefulBits >> 16);
    return (xored * 65521) + usefulBits;
}

static unsigned nextIndex(_PPLCache *self, unsigned idx) {
    return (idx+1 >= self->max) ? 0 : idx+1;
}

- init {
    if (!deleted) {
	deleted = NSZoneMalloc(NULL, sizeof(_PPLCacheEntry));
	memset(deleted, 0, sizeof(_PPLCacheEntry));
    }
    max = MIN_CACHE_MAX;	// must be 2**N - 1
    oentries = NSZoneMalloc(NULL, max * sizeof(_PPLCacheEntry *));
    memset(oentries, 0, max * sizeof(_PPLCacheEntry *));
    hentries = NSZoneMalloc(NULL, max * sizeof(_PPLCacheEntry *));
    memset(hentries, 0, max * sizeof(_PPLCacheEntry *));
    initialDate = [[NSDate allocWithZone:NULL] init];
    currentGenerationTime = 0.0;
    halfLife = 20.0;
    if (!allCaches) allCaches = NSCreateHashTableWithZone(NSNonRetainedObjectHashCallBacks, 1, NULL);
    if (_PPLAllCachesLock) [_PPLAllCachesLock lock];
    NSHashInsert(allCaches, self);
    if (_PPLAllCachesLock) [_PPLAllCachesLock unlock];
    return self;
}

+ (void)noteDeallocatingProxy:object {
    if (_PPLAllCachesLock) [_PPLAllCachesLock lock];
    if (allCaches) {
	NSHashEnumerator	state = NSEnumerateHashTable(allCaches);
	_PPLCache		*cache;
	while ((cache = (id)NSNextHashEnumeratorItem(&state))) {
	    [cache removeCachedObject:object];
	}
    }
    if (_PPLAllCachesLock) [_PPLAllCachesLock unlock];
}

- (void)setHalfLife:(NSTimeInterval)hh {
    halfLife = hh;
}

- (void)dealloc {
    if (_PPLAllCachesLock) [_PPLAllCachesLock lock];
    NSHashRemove(allCaches, self);
    if (_PPLAllCachesLock) [_PPLAllCachesLock unlock];
    [self removeAllCachedObjects];
    NSZoneFree(NSZoneFromPointer(oentries), oentries);
    NSZoneFree(NSZoneFromPointer(hentries), hentries);
    [initialDate release];
    [super dealloc];
}

static double residue = 0.5; // non-random probability residue, always <1.0

static double powArg = 0.0;
static double powResult = 1.0;

#if !defined(M_LN2)
#define M_LN2   0.69314718055994530942
#endif

static BOOL shouldKeepPair(_PPLCache *self, _PPLCacheEntry *entry) {
    double delta = entry->lastAccessTime - self->currentGenerationTime; // <0 
    double proba = 0.0; // <1.0
    if (!delta) return YES;
    if (self->halfLife > 0) {
	double	divisor;
	if (!delta) {
	    proba = 1.0;
	} else if ((divisor = delta / self->halfLife) == powArg) {
	    // pow is slow, so we cache the last result
	    proba = powResult;
	} else {
	    powArg = divisor;
	    /* cjk: changed pow(2.0, powArg) --> exp(M_LN2 * divisor) */
	    powResult = exp(M_LN2 * divisor);
	    proba = powResult;
	}
    }
    residue += proba;
    if (residue >= 1.0) {
	/* we keep that entry! */
	residue -= 1.0;
	entry->lastAccessTime = self->currentGenerationTime;
	return YES;
    }
    return NO;
}

static BOOL purgeChain(_PPLCache *self, _PPLCacheEntry **entries, unsigned from, unsigned limit) {
    unsigned	next;
    /* returns YES if chain starting at from is empty */
    if (!entries[from]) return YES;
    next = nextIndex(self, from);
    if (next == limit) return NO;
    if (!purgeChain(self, entries, next, limit)) return NO;
    if (entries[from] != deleted) return NO;
    entries[from] = NULL;
    return YES;
}

static _PPLCacheEntry *entryForObject(_PPLCache *self, id object) {
    unsigned		idx = OHASH(object) % self->max;
    unsigned		idx2 = idx;
    _PPLCacheEntry	*entry = self->oentries[idx];
    if (!entry) return NULL;
    if (entry->object == object) return entry;
    purgeChain(self, self->oentries, idx, idx);
    while ((idx2 = nextIndex(self, idx2)) != idx) {
	entry = self->oentries[idx2];
	if (!entry) return NULL;
	if (entry->object == object) return entry;
    }
    return NULL;
}

static _PPLCacheEntry *entryForHandle(_PPLCache *self, unsigned handle) {
    unsigned		idx = HHASH(handle) % self->max;
    unsigned		idx2 = idx;
    _PPLCacheEntry	*entry = self->hentries[idx];
    if (!entry) return NULL;
    if (entry->handle == handle) return entry;
    purgeChain(self, self->hentries, idx, idx);
    while ((idx2 = nextIndex(self, idx2)) != idx) {
	entry = self->hentries[idx2];
	if (!entry) return NULL;
	if (entry->handle == handle) return entry;
    }
    return NULL;
}

- (unsigned)cachedHandleForObject:object {
    _PPLCacheEntry	*entry = entryForObject(self, object);
    if (!entry) return 0;
    entry->lastAccessTime = currentGenerationTime;
    return entry->handle;
}

- cachedObjectForHandle:(unsigned)handle {
    _PPLCacheEntry	*entry = entryForHandle(self, handle);
    if (!entry) return nil;
    entry->lastAccessTime = currentGenerationTime;
    return entry->object;
}

- (void)_grow {
    _PPLCacheEntry	**oldHentries = hentries;
    unsigned		oldMax = max;
    max += max + 1;
    if (_PPLCacheLog) PPLLog(@"== growing to %d for count=%d", max, count);
    count = 0;
    oentries = NSZoneRealloc(NSZoneFromPointer(oentries), oentries, max * sizeof(_PPLCacheEntry *));
    memset(oentries, 0, max * sizeof(_PPLCacheEntry *));
    hentries = NSZoneMalloc(NULL, max * sizeof(_PPLCacheEntry *));
    memset(hentries, 0, max * sizeof(_PPLCacheEntry *));
    addEntries(self, oldHentries, oldMax);
    NSZoneFree(NSZoneFromPointer(oldHentries), oldHentries);
}

- (void)_shrink {
    _PPLCacheEntry	**oldHentries = hentries;
    unsigned		oldMax = max;
    if (max / 2 <= MIN_CACHE_MAX) return;
    max = max / 2;
    if (_PPLCacheLog) PPLLog(@"== shrinking to %d for count=%d", max, count);
    count = 0;
    NSZoneFree(NSZoneFromPointer(oentries), oentries);
    oentries = NSZoneMalloc(NULL, max * sizeof(_PPLCacheEntry *));
    memset(oentries, 0, max * sizeof(_PPLCacheEntry *));
    hentries = NSZoneMalloc(NULL, max * sizeof(_PPLCacheEntry *));
    memset(hentries, 0, max * sizeof(_PPLCacheEntry *));
    addEntries(self, oldHentries, oldMax);
    NSZoneFree(NSZoneFromPointer(oldHentries), oldHentries);
}

static void swap(_PPLCacheEntry **ref1, _PPLCacheEntry **ref2) {
    _PPLCacheEntry	*temp = *ref1;
    *ref1 = *ref2;
    *ref2 = temp;
}

static void addEntry(_PPLCache *self, _PPLCacheEntry *entryToAdd) {
    /* We first add to oentries, then to hentries, then grow if necessary;
    entryToAdd may be freed (if already in cache) */
    unsigned		idx = OHASH(entryToAdd->object) % self->max;
    unsigned		idx2 = idx;
    _PPLCacheEntry	*entry = self->oentries[idx];
    /* Part 1 */
    if (!entry || (entry == deleted)) {
	self->oentries[idx] = entryToAdd;
	goto part2;
    }
    if (entry->handle == entryToAdd->handle) goto alreadyInTable;
    if (self->count == self->max) {
	[self refreshCache];
	/* no room: rehash and retry */
	if (self->count == self->max) [self _grow];
	addEntry(self, entryToAdd);
	return;
    }
    purgeChain(self, self->oentries, idx, idx);
    while ((idx2 = nextIndex(self, idx2)) != idx) {
	entry = self->oentries[idx2];
	if (!entry || (entry == deleted)) {
	    _PPLCacheEntry	*current = entryToAdd;
	    idx2 = idx;
	    while (current && (current != deleted)) {
		swap(self->oentries + idx2, &current);
		idx2 = nextIndex(self, idx2);
	    }
	    goto part2;
	}
	if (entry->handle == entryToAdd->handle) goto alreadyInTable;
    }
    [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache addEntry() 1"];
  alreadyInTable:
    // it is possible to enter a different proxy on the same object because of the following scenario: get a string off a PPL, retain, flush the cache, get the same string, enter in cache.
    entry->lastAccessTime = self->currentGenerationTime;
    NSZoneFree(NSZoneFromPointer(entryToAdd), entryToAdd);
    return;
  part2:
    idx = HHASH(entryToAdd->handle) % self->max;
    idx2 = idx;
    entry = self->hentries[idx];
    if (!entry || (entry == deleted)) {
	self->hentries[idx] = entryToAdd;
	goto part3;
    }
    if (entry->handle == entryToAdd->handle) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache addEntry() 2"];
    if (self->count == self->max) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache addEntry() 3"];
    purgeChain(self, self->hentries, idx, idx);
    while ((idx2 = nextIndex(self, idx2)) != idx) {
	entry = self->hentries[idx2];
	if (!entry || (entry == deleted)) {
	    _PPLCacheEntry	*current = entryToAdd;
	    idx2 = idx;
	    while (current && (current != deleted)) {
		swap(self->hentries + idx2, &current);
		idx2 = nextIndex(self, idx2);
	    }
	    goto part3;
	}
	if (entry->handle == entryToAdd->handle) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache addEntry() 3"];
    }
    [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache addCachedObject() 4"];
  part3:
    self->count++;
    if (self->shouldRefresh) {
	[self refreshCache];
	if (self->count * 100 < self->max * REHASH_THRESHOLD / 4) [self _shrink];
    }
    if (self->count * 100 >= self->max * REHASH_THRESHOLD) [self _grow];
}

- (void)addCachedObject:object handle:(unsigned)handle {
    _PPLCacheEntry	*entry = NSZoneMalloc(NULL, sizeof(_PPLCacheEntry));
    if (!shouldRefresh && (count >= MIN_CACHE_MAX) && (currentGenerationCounter++ >= max / 2)) {
	currentGenerationTime = - [initialDate timeIntervalSinceNow];
	currentGenerationCounter = 0;
	shouldRefresh = YES;
    }
    entry->object = object; entry->handle = handle; 
    entry->lastAccessTime = currentGenerationTime; 
    addEntry(self, entry);
}

static void removeEntry(_PPLCache *self, _PPLCacheEntry *entryToRemove) {
    unsigned		idx = OHASH(entryToRemove->object) % self->max;
    unsigned		idx2 = idx;
    _PPLCacheEntry	*entry = self->oentries[idx];
    if (!entry) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache removeEntry() 1"];
    if (entry == entryToRemove) goto ofound;
    purgeChain(self, self->oentries, idx, idx);
    while ((idx2 = nextIndex(self, idx2)) != idx) {
	entry = self->oentries[idx2];
	if (!entry) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache removeEntry() 2"];
	if (entry == entryToRemove) {
	    idx = idx2;
	    goto ofound;
	}
    }
    return;
  ofound:
    idx2 = nextIndex(self, idx);
    if ((idx2 == idx) || !self->oentries[idx2]) {
	/* we can just blast */
	self->oentries[idx] = NULL;
    } else {
	self->oentries[idx] = deleted;
    }
    /* we now do the same for hentries */
    idx = HHASH(entryToRemove->handle) % self->max;
    idx2 = idx;
    entry = self->hentries[idx];
    if (!entry) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache removeEntry() 3"];
    if (entry == entryToRemove) goto hfound;
    purgeChain(self, self->hentries, idx, idx);
    while ((idx2 = nextIndex(self, idx2)) != idx) {
	entry = self->hentries[idx2];
	if (!entry) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache removeEntry() 4"];
	if (entry == entryToRemove) {
	    idx = idx2;
	    goto hfound;
	}
    }
    [NSException raise:NSInternalInconsistencyException format:@"*** _PPLCache removeCachedObject() 3"];
  hfound:
    idx2 = nextIndex(self, idx);
    if ((idx2 == idx) || !self->hentries[idx2]) {
	/* we can just blast */
	self->hentries[idx] = NULL;
    } else {
	self->hentries[idx] = deleted;
    }
    self->count--;
    NSZoneFree(NSZoneFromPointer(entryToRemove), entryToRemove);
}

- (void)removeCachedObject:object {
    _PPLCacheEntry	*entry = entryForObject(self, object);
    if (entry) removeEntry(self, entry);
}

- (void)removeCachedObjectForHandle:(unsigned)handle {
    _PPLCacheEntry	*entry = entryForHandle(self, handle);
    if (entry) removeEntry(self, entry);
}

- (void)refreshCache {
    _PPLCacheEntry	**entries = NSZoneMalloc(NULL, count * sizeof(_PPLCacheEntry *));
    unsigned	idx = max;
    unsigned	num = 0;
    while (idx--) {
	_PPLCacheEntry	*entry = hentries[idx];
	if (!entry || (entry == deleted)) {
	    /* nothing */
	} else if (shouldKeepPair(self, entry)) {
	    entries[num++] = entry;
	} else {
	    NSZoneFree(NSZoneFromPointer(entry), entry);
	}
    }
    if (_PPLCacheLog) PPLLog(@"== refreshing cache (max=%d) count=%d", max, count);
    shouldRefresh = NO; // important to do that before re-inserting
    count = 0;
    memset(oentries, 0, max * sizeof(_PPLCacheEntry *));
    memset(hentries, 0, max * sizeof(_PPLCacheEntry *));
    addEntries(self, entries, num);
    NSZoneFree(NSZoneFromPointer(entries), entries);
    if (_PPLCacheLog) PPLLog(@"== refreshed; new cache count=%d", count);
}

- (void)removeAllCachedObjects {
    unsigned	idx = max;
    while (idx--) {
	_PPLCacheEntry	*entry = hentries[idx];
	if (entry && (entry != deleted)) {
	    NSZoneFree(NSZoneFromPointer(entry), entry);
	}
    }
    count = 0;
    memset(oentries, 0, max * sizeof(_PPLCacheEntry *));
    memset(hentries, 0, max * sizeof(_PPLCacheEntry *));
}

- (unsigned)count {
    return count;
}

- (void)getAllCachedObjectsInto:(id *)objects handles:(unsigned *)handles max:(unsigned)omax {
    unsigned	idx = max;
    while (omax && idx--) {
	_PPLCacheEntry	*entry = hentries[idx];
	if (entry && (entry != deleted)) {
	    *objects = entry->object; objects++; 
	    *handles = entry->handle; handles++;
	    omax--;
	}
    }
}

@end

/***************	Utilities for specific Sorted Lists	***********/

static SortedNumToVoids *createSortedNumToVoids(unsigned capacity);
static void freeSortedNumToVoids(SortedNumToVoids *spairs);
static NumToVoid *removeLastNumToVoid(SortedNumToVoids *spairs);
static unsigned smallestIxGreaterOrEqual(SortedNumToVoids *spairs, unsigned num);
static NumToVoid *smallestNumToVoidGreaterOrEqual(SortedNumToVoids *spairs, unsigned num);
static NumToVoid *largestNumToVoidLess(SortedNumToVoids *spairs, unsigned num);
static void removeNumToVoidEqualTo(SortedNumToVoids *spairs, unsigned num);
static void addNumToVoid(SortedNumToVoids *spairs, unsigned num, void *data);

/***************	Utilities for specific Sorted Lists	***********/

static SortedNumToVoids *createSortedNumToVoids(unsigned capacity) {
    SortedNumToVoids	*spairs = NSZoneMalloc(NULL, sizeof(SortedNumToVoids));
    spairs->count = 0;
    spairs->max = capacity;
    spairs->pairs = NSZoneMalloc(NULL, sizeof(NumToVoid) * capacity);
    return spairs;
}

static void freeSortedNumToVoids(SortedNumToVoids *spairs) {
    NSZoneFree(NSZoneFromPointer(spairs->pairs), spairs->pairs);
    NSZoneFree(NSZoneFromPointer(spairs), spairs);
}

static NumToVoid *removeLastNumToVoid(SortedNumToVoids *spairs) {
    /* caller must copy pair */
    if (!spairs->count) {
	return NULL;
    } else {
	spairs->count--;
	return spairs->pairs + spairs->count;
    }
}

static unsigned smallestIxGreaterOrEqual(SortedNumToVoids *spairs, unsigned num) {
    /* very private */
    unsigned	first = 0;
    unsigned	last = spairs->count;
    while (first < last) {
	unsigned	half = (first + last) / 2;
	unsigned	numHalf = spairs->pairs[half].num;
	if (numHalf < num) {
	    first = half + 1;
	} else if (numHalf == num) {
	    return half;
	} else if (last != half + 1) {
	    last = half + 1;
	} else if ((first != half) && (spairs->pairs[first].num >= num)) {
	    return first;
	} else {
	    return half;
	}
    }
    return spairs->count;
}

static NumToVoid *smallestNumToVoidGreaterOrEqual(SortedNumToVoids *spairs, unsigned num) {
    /* caller must copy pair */
    unsigned	idx = smallestIxGreaterOrEqual(spairs, num);
    if (idx == spairs->count) return NULL;
    return spairs->pairs + idx;
}

static NumToVoid *largestNumToVoidLess(SortedNumToVoids *spairs, unsigned num) {
    unsigned	idx = smallestIxGreaterOrEqual(spairs, num);
    if (!idx) return NULL;
    return spairs->pairs + idx - 1;
}

static void removeNumToVoidEqualTo(SortedNumToVoids *spairs, unsigned num) {
    unsigned	idx = smallestIxGreaterOrEqual(spairs, num);
    if ((idx == spairs->count) || (spairs->pairs[idx].num != num)) [NSException raise:NSInternalInconsistencyException format:@"*** removeNumToVoidEqualTo()"];
    spairs->count --;
    if (idx != spairs->count) {
	NumToVoid	*dest = spairs->pairs + idx;
	NumToVoid	*source = dest + 1;
	memmove(dest, source, (spairs->count - idx) * sizeof(NumToVoid));
    }
}

static void addNumToVoid(SortedNumToVoids *spairs, unsigned num, void *data) {
    unsigned	idx = smallestIxGreaterOrEqual(spairs, num);
    NumToVoid	*pair;
    if (spairs->count >= spairs->max) {
	spairs->max += spairs->max + 1;
	spairs->pairs = NSZoneRealloc(NSZoneFromPointer(spairs->pairs), spairs->pairs, spairs->max * sizeof(NumToVoid));
    }
    pair = spairs->pairs + idx;
    if (idx != spairs->count) {
	NumToVoid	*dest = pair + 1;
        if (pair->num <= num) [NSException raise:NSInternalInconsistencyException format:@"*** addNumToVoid()"];
	memmove(dest, pair, (spairs->count - idx) * sizeof(NumToVoid));
    }
    pair->num = num; pair->data = data;
    spairs->count++;
}

/***************	Free Information	***************/

@implementation _PPLFreeInfo

- init {
    free1 = [[_SortedNums allocWithZone:NULL] init];
    freeLengthToHeap = createSortedNumToVoids(2);
    freeToLength = createSortedNumToVoids(2);
    return self;
}

- (void)dealloc {
    NumToVoid	*pair;
    [free1 release];
    while ( (pair = removeLastNumToVoid(freeLengthToHeap)) ) {
	PPLFreeOrderedHeap(pair->data);
    }
    freeSortedNumToVoids(freeLengthToHeap);
    freeSortedNumToVoids(freeToLength);
    [super dealloc];
}

- (void)checkHeapRec:(PPLOrderedHeap *)heap length:(unsigned)length count:(unsigned *)count inStore:(_PPLStore *)store {
    unsigned	handle = (unsigned)PPLRemoveMinimumOfOrderedHeap(heap);
    if (!handle) {
	return;
    } else {
	NumToVoid	*pair = smallestNumToVoidGreaterOrEqual(freeToLength, handle);
	if (handle != pair->num) [NSException raise:NSInternalInconsistencyException format:@"*** %@ 1", NSStringFromSelector(_cmd)];
	if (length != (unsigned)pair->data) [NSException raise:NSInternalInconsistencyException format:@"*** %@ 2", NSStringFromSelector(_cmd)];
	if (length != [store totalRoundedLengthOfBlock:handle]) [NSException raise:NSInternalInconsistencyException format:@"*** %@ 3", NSStringFromSelector(_cmd)];
	(*count)++;
	[self checkHeapRec:heap length:length count:count inStore:store];
	PPLAddToOrderedHeap(heap, (void *)handle);
    }
}

- (void)checkInStore:(_PPLStore *)store {
    unsigned	count = 0;
    NumToVoid	*pair;
    unsigned	length = 0;
    while ( (pair = smallestNumToVoidGreaterOrEqual(freeLengthToHeap, length)) ) {
	length = pair->num;
	[self checkHeapRec:pair->data length:length count:&count inStore:store];
	length ++;
    }
    if (freeToLength->count != count) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
}

static void addFree(_PPLFreeInfo *self, unsigned handle, unsigned length) {
    /* no merging */
    NumToVoid		*pair;
    PPLOrderedHeap	*heap = NULL;
    if (length == MIN_CHUNK) {
	[self->free1 addUnsigned:handle];
	return;
    }
    pair = smallestNumToVoidGreaterOrEqual(self->freeLengthToHeap, length);
    if (pair && (pair->num == length)) {
	heap = pair->data;
    } else {
	heap = PPLCreateOrderedHeap(NULL, 1);
	addNumToVoid(self->freeLengthToHeap, length, heap);
    }
    PPLAddToOrderedHeap(heap, (void *)handle);
    addNumToVoid(self->freeToLength, handle, (void *)length);
}

static void removeFreeInNumToVoid(_PPLFreeInfo *self, unsigned handle, NumToVoid *pair) {
    /* no merging */
    PPLOrderedHeap	*heap = pair->data;
    if (!PPLRemoveLeastGreaterOfOrderedHeap(heap, (void *)handle)) {
	[NSException raise:NSInternalInconsistencyException format:@"*** _PPLFreeInfo removeFree(): not found"];
    }
    if (!PPLCountOrderedHeap(heap)) {
	removeNumToVoidEqualTo(self->freeLengthToHeap, pair->num);
	PPLFreeOrderedHeap(heap);
    }
    removeNumToVoidEqualTo(self->freeToLength, handle);
}

static void removeFree(_PPLFreeInfo *self, unsigned handle, unsigned length) {
    /* no merging */
    NumToVoid		*pair;
    if (length == MIN_CHUNK) {
	[self->free1 removeUnsigned:handle];
	return;
    }
    pair = smallestNumToVoidGreaterOrEqual(self->freeLengthToHeap, length);
    removeFreeInNumToVoid(self, handle, pair);
}

static unsigned tryMergeWithNext(_PPLFreeInfo *self, unsigned handle, unsigned length, unsigned nextHandle, unsigned nextLength, _PPLStore *store) {
    /* returns 0 if can't merge, next handle otherwise */
    if (nextHandle == handle) return handle + length; /* already in table! */
    if (nextHandle < handle + length) [NSException raise:NSInternalInconsistencyException format:@"*** tryMergeWithNext() handle=^0x length=%d nextHandle=^%x", handle, length, nextHandle];
    if ([store totalRoundedLengthOfBlock:nextHandle] != nextLength) [NSException raise:NSInternalInconsistencyException format:@"*** tryMergeWithNext() for nextHandle=^%x, length=%d", nextHandle, [store totalRoundedLengthOfBlock:nextHandle]];
    if (handle + length == nextHandle) {
	/* we merge with next */
	removeFree(self, nextHandle, nextLength);
	[store mergeFreeBlockWithNext:handle];
	return [self noteFree:handle inStore:store];
    }
    return 0;
}

static unsigned tryMergeWithPrevious(_PPLFreeInfo *self, unsigned handle, unsigned length, unsigned previousHandle, unsigned previousLength, _PPLStore *store) {
    /* returns 0 if can't merge, next handle otherwise */
    if ([store totalRoundedLengthOfBlock:previousHandle] != previousLength) [NSException raise:NSInternalInconsistencyException format:@"*** tryMergeWithPrevious()"];
    if (previousHandle + previousLength == handle) {
	/* we merge with previous */
	removeFree(self, previousHandle, previousLength);
	[store mergeFreeBlockWithNext:previousHandle];
	return [self noteFree:previousHandle inStore:store];
    }
    return 0;
}

- (unsigned)noteFree:(unsigned)handle inStore:(_PPLStore *)store {
    unsigned	length = [store totalRoundedLengthOfBlock:handle];
    NumToVoid	*pair;
    unsigned	nextHandle;
    unsigned	previousHandle;
    nextHandle = [free1 smallestGreaterOrEqual:handle];
    if (nextHandle) {
	unsigned	ret;
	ret = tryMergeWithNext(self, handle, length, nextHandle, MIN_CHUNK, store);
	if (ret) return ret;
    }
    pair = smallestNumToVoidGreaterOrEqual(freeToLength, handle);
    if (pair) {
	unsigned	ret;
	ret = tryMergeWithNext(self, handle, length, pair->num, (unsigned)pair->data, store);
	if (ret) return ret;
    }
    pair = largestNumToVoidLess(freeToLength, handle);
    if (pair) {
	unsigned	ret;
	ret = tryMergeWithPrevious(self, handle, length, pair->num, (unsigned)pair->data, store);
	if (ret) return ret;
    }
    previousHandle = [free1 largestLesser:handle];
    if (previousHandle) {
	unsigned	ret;
	ret = tryMergeWithPrevious(self, handle, length, previousHandle, MIN_CHUNK, store);
	if (ret) return ret;
    }
    addFree(self, handle, length);
    //[self checkInStore:store];
    return handle + length;
}

- (unsigned)findFreeBlock:(unsigned)wantedLength cold:(BOOL)cold inStore:(_PPLStore *)store {
    NumToVoid	*pair;
    if (wantedLength <= MIN_CHUNK) {
	unsigned	handle;
	if (cold) {
	    handle = [free1 removeSmallest];
	} else {
	    handle = [free1 removeLargest];
	}
	if (handle) {
	    //[self checkInStore:store];
	    return handle;
	}
    }
    pair = smallestNumToVoidGreaterOrEqual(freeLengthToHeap, wantedLength);
    if (pair) {
	unsigned	length = pair->num;
	PPLOrderedHeap	*heap = pair->data;
	unsigned	handle;
	if (cold) {
	    handle = (unsigned)PPLRemoveMinimumOfOrderedHeap(heap);
	} else {
	    handle = (unsigned)PPLRemoveRandomInOrderedHeap(heap);
	}
	if (!handle) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
	if ([store totalRoundedLengthOfBlock:handle] != length) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
	if (!PPLCountOrderedHeap(heap)) {
	    removeNumToVoidEqualTo(freeLengthToHeap, length);
	    PPLFreeOrderedHeap(heap);
	}
	removeNumToVoidEqualTo(freeToLength, handle);
	if (length > wantedLength) {
	    /* we split block in 2 free blocks */
	    unsigned	nextHandle;
	    unsigned	nextHandleLength;
	    nextHandle = [store splitFreeBlockInTwo:handle atIndex:wantedLength];
	    nextHandleLength = length - wantedLength;
	    if ([store totalRoundedLengthOfBlock:nextHandle] != nextHandleLength) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
	    /* we add in the current (usable) free list */
	    addFree(self, nextHandle, nextHandleLength);
	}
	//[self checkInStore:store];
	return handle;
    }
    return 0;
}

static void dumpHeapRec(PPLOrderedHeap *heap, NSMutableString *res) {
    unsigned	handle = (unsigned)PPLRemoveMinimumOfOrderedHeap(heap);
    if (!handle) {
	return;
    } else {
	[res appendStoreHandle:handle];
	dumpHeapRec(heap, res);
	PPLAddToOrderedHeap(heap, (void *)handle);
    }
}

static NSString *dumpHeap(PPLOrderedHeap *heap) {
    NSMutableString	*res = [[NSMutableString allocWithZone:NULL] init];
    dumpHeapRec(heap, res);
    return [res autorelease];
}

- (NSString *)description {
    NSMutableString	*res = [[NSMutableString allocWithZone:NULL] init];
    NumToVoid	*pair;
    unsigned	length = 0;
    [res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@"== Blocks of size MIN_CHUNK: "];
    [res replaceCharactersInRange:NSMakeRange([res length], 0) withString:[free1 description]];
    [res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@"\n"];
    while ( (pair = smallestNumToVoidGreaterOrEqual(freeLengthToHeap, length)) ) {
	NSString	*temp;
	temp = [[NSString allocWithZone:NULL] initWithFormat:@"== Free blocks for length %d: %@\n", pair->num, dumpHeap(pair->data)];
	[res replaceCharactersInRange:NSMakeRange([res length], 0) withString:temp];
	[temp release];
	length = pair->num+1;
    }
    return [res autorelease];
}

@end

/***************	Definitions	***************/

/* The log format denotes a suite of physical transactions that span a
single logical transactiuon that can be undone; each transaction is a
suite of log entries ending with a pair (LOG_MARKER, length of the data
file when last consistent); A log entry is either a tuple that indicates
how to undo 1,2,3 or 4 consecutive words, or a pair (address+1, length
of the free block at that address); There can be several pairs with the
same address: the first one is the true original value (which implies
that transaction need to be restored in reverse); A broken physical
transaction does not end the way it should: it is not replayed;
0 is not a valid log entry marker, which makes us eliminate the need 
for having a sequence number of the log entries for the case where the
asynchronous writes would be executed out of order and crash */

#define LOG_MARKER	0xfffffffe	// changing this changes file format

#define LOG_FREE_MARKER	0xfffffffd	// changing this changes file format

#define SEEK_THRESH	1024	// tradeoff more seeking <-> more writing

static BOOL _tryOptimizeLog = YES;

/***************	Utilities	***************/

static BOOL canBeHandle(unsigned num) {
    return !(num & (MIN_CHUNK - 1)) && (num >= BLOCK_HEADER_LEN);
}

static BOOL isWordAligned(unsigned num) {
    return !(num & (WORD_SIZE - 1));
}

static unsigned numValues(unsigned logAddress) {
    unsigned	numVal = logAddress & 3;
    return (numVal) ? numVal : 4;
}

static BOOL canCoalesce(NSRange range1, NSRange range2, NSRange *result) {
    if (range1.location <= range2.location) {
	if (NSMaxRange(range1) + SEEK_THRESH >= range2.location) {
	    unsigned	end = NSMaxRange(range2);
	    NSRange	temp = range1;
	    if (end > NSMaxRange(range1)) {
		temp.length = end - range1.location;
	    }
	    *result = temp;
	    return YES;
	}
	return NO;
    } else {
	if (NSMaxRange(range2) + SEEK_THRESH >= range1.location) {
	    unsigned	end = NSMaxRange(range1);
	    NSRange	temp = range2;
	    if (end > NSMaxRange(range2)) {
		temp.length = end - range2.location;
	    }
	    *result = temp;
	    return YES;
	}
	return NO;
    }
}

static void addAndTryCoalesceWithLast(_PPLRangeArray *ranges, NSRange range) {
    NSRange	*last = ranges->ranges + ranges->count - 1;
    if (!ranges->count || !canCoalesce(*last, range, last)) {
	/* just start a new range ! */
	AddToPPLRangeArray(ranges, range, _PPLRangeArrayAppend);
    }
}

static unsigned parseLogData(NSData *logData, LogEntry **entries) {
    /* returns number of entries */
    unsigned	numEntries = 0;
    unsigned	logDataLength = [logData length];
    unsigned	count = logDataLength / WORD_SIZE;
    unsigned	*nums = NSZoneMalloc(NULL, (count + 5) * sizeof(unsigned)); // we over dimension by 4 to allow poking beyond the logical end
    BOOL	appendBrokenEntry = NO;
    unsigned	idx = 0;
    unsigned	maxEntries = 0;
    *entries = NULL;
    if (logDataLength & (WORD_SIZE - 1)) {
	PPLLog(@"*** Inconsistent log file; Skipping last bytes ...");
	appendBrokenEntry = YES;
    }
    [logData deserializeInts:nums count:count atIndex:0];
    while (idx < count) {
	unsigned	num1 = nums[idx++];
	unsigned	num2 = nums[idx++];
	BOOL		valid = YES;
	LogEntry	*entry;
	if (numEntries + 1 > maxEntries) {
	    maxEntries += maxEntries + 1;
	    *entries = NSZoneRealloc(NSZoneFromPointer(*entries), *entries, maxEntries * sizeof(LogEntry));
	}
	entry = (*entries) + numEntries;
        memset(entry, 0, sizeof(LogEntry)); /* to ease debug */
	if (num1 == LOG_MARKER) {
	    if (!canBeHandle(num2)) {
		PPLLog(@"*** parseLogData: not a proper handle for marker: 0x%x", num2);
		valid = NO;
	    }
	    entry->type = LogEntryMarker;
	    entry->address = num2;
	} else if (num1 == LOG_FREE_MARKER) {
	    num1 = num2; num2 = nums[idx++];
	    if (!isWordAligned(num1)) {
		PPLLog(@"*** parseLogData: not a proper address for free: 0x%x", num1);
		valid = NO;
	    }
	    if (!isWordAligned(num2)) {
		PPLLog(@"*** parseLogData: not a proper length for free: 0x%x", num2);
		valid = NO;
	    }
	    entry->type = LogEntryFree;
	    entry->address = num1;
	    entry->num0 = num2;
	} else {
	    unsigned	numVal = numValues(num1);
	    entry->type = LogEntryChange;
	    entry->address = num1;
	    entry->num0 = num2;
	    if (numVal > 1) entry->num1 = nums[idx++];
	    if (numVal > 2) entry->num2 = nums[idx++];
	    if (numVal > 3) entry->num3 = nums[idx++];
	}
	if (idx > count) valid = NO;
	if (!valid) entry->type = LogEntryInvalid;
	numEntries++;
    }
    if (appendBrokenEntry) {
	if (numEntries + 1 > maxEntries) {
	    maxEntries += maxEntries + 1;
	    *entries = NSZoneRealloc(NSZoneFromPointer(*entries), *entries, maxEntries * sizeof(LogEntry));
	}
	(*entries)[numEntries].type = LogEntryInvalid;
	numEntries++;
    }
    NSZoneFree(NSZoneFromPointer(nums), nums);
    return numEntries;
}

static void undoLogEntry(NSMutableData *current, LogEntry *entry) {
    unsigned	currentLength = [current length];
    switch (entry->type) {
	case LogEntryChange: {
	    unsigned	address = entry->address;
	    unsigned	numVal = numValues(address);
	    address = address & ~ 3U;
	    /* there could be entries that are beyond the end of the file because they were changes to blocks added in that transaction; we filter these out */
	    if (address + WORD_SIZE <= currentLength) {
		[current serializeInt:(int)entry->num0 atIndex:address];
		address += WORD_SIZE;
	    }
	    if ((numVal > 1) && (address + WORD_SIZE <= currentLength)) {
		[current serializeInt:(int)entry->num1 atIndex:address];
		address += WORD_SIZE;
	    }
	    if ((numVal > 2) && (address + WORD_SIZE <= currentLength)) {
		[current serializeInt:(int)entry->num2 atIndex:address];
		address += WORD_SIZE;
	    }
	    if ((numVal > 3) && (address + WORD_SIZE <= currentLength)) {
		[current serializeInt:(int)entry->num3 atIndex:address];
	    }
	    break;
	}
	case LogEntryFree: {
	    unsigned	address = entry->address;
	    unsigned	length = entry->num0;
	    if (length & 3) [NSException raise:NSInternalInconsistencyException format:@"***  undoLogEntry() Unexpected log value"];
	    if (address < currentLength) {
		/* we ignore changes beyond end of file */
		unsigned	numBytes = currentLength - address;
		if (numBytes > length) numBytes = length;
                memset(((char *)[current mutableBytes])+address, 0, numBytes);
	    }
	    break;
	}
	default:
	    [NSException raise:NSInternalInconsistencyException format:@"*** undoLogEntry()"];
    }
}

static NSString *stringForLogEntry(LogEntry *entry) {
    switch (entry->type) {
	case LogEntryMarker:
	    return [NSString stringWithFormat:@"MARKER to 0x%x", entry->address];
	case LogEntryChange: {
	    NSMutableString	*res = [[NSMutableString allocWithZone:NULL] init];
	    unsigned	address = entry->address;
	    unsigned	numVal = numValues(address);
	    address = address & ~ 3U;
	    [res appendFormat:@"CHANGE 0x%x: 0x%x", address, entry->num0];
	    if (numVal > 1) [res appendFormat:@" 0x%x", entry->num1];
	    if (numVal > 2) [res appendFormat:@" 0x%x", entry->num2];
	    if (numVal > 3) [res appendFormat:@" 0x%x", entry->num3];
	    return [res autorelease];
	}
	case LogEntryFree:
	    return [NSString stringWithFormat:@"FREE 0x%x: length=0x%x", entry->address, entry->num0];
	default:
	    return [NSString stringWithCString:"INVALID" length:7];
    }
}

static NSString *stringForLogEntries(LogEntry *entries, unsigned numEntries) {
    NSMutableString	*res = [[NSMutableString allocWithZone:NULL] init];
    while (numEntries--) {
	[res replaceCharactersInRange:NSMakeRange([res length], 0) withString:stringForLogEntry(entries)];
	[res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@"\n"];
	entries++;
    }
    return [res autorelease];
}

static BOOL scavenge(LogEntry *entries, unsigned numEntries, NSMutableData *current) {
    LogEntry	*entry;
    if (!numEntries) goto nope;
    entry = entries + numEntries - 1;
    PPLLog(@"Undo log contains %d entries", numEntries);
    if (entry->type != LogEntryMarker) {
	PPLLog(@"*** Incomplete last transaction; skipping");
	while ((entry != entries) && (entry->type != LogEntryMarker)) {
	    entry--;
	}
	if (entry->type != LogEntryMarker) goto nope;
    }
    while (1) {
	if (entry->address < [current length]) {
	    PPLLog(@"*** Back-integrating transaction; truncating at 0x%x", entry->address); 
	    [current setLength:entry->address];
	}
	if (entry == entries) break;
	entry--;
	while ((entry != entries) && (entry->type != LogEntryMarker)) {
	    if (entry->type == LogEntryInvalid) {
		PPLLog(@"*** Incorrect data encountered in log; aborting");
		goto nope;
	    }
	    undoLogEntry(current, entry);
	    entry--;
	}
	PPLLog(@"Undid physical transaction ...");
    }
    PPLLog(@"Scavenging complete");
    return YES;
  nope:
    return NO;
}

static NSRange logEntryRange(LogEntry *entry) {
    NSRange		range;
    switch (entry->type) {
	case LogEntryChange: {
	    unsigned	numVal = numValues(entry->address);
	    range.location = entry->address & ~3U; 
	    range.length = WORD_SIZE * numVal;
	    break;
	}
	case LogEntryFree:
	    range.location = entry->address; range.length = entry->num0;
	    break;
	default:
	    [NSException raise:NSInternalInconsistencyException format:@"*** Encountered unexpected entry in log: %d; file corruption is likely", entry->type];
    }
    return range;
}

static void coalesceAndSortRanges(_PPLRangeArray *ranges) {
    /* Given an array containing the ranges that changed, 
    returns the number of ranges to copy and sets these coalesced ranges;
    we sort to diminish disk spinning */
    unsigned		numRanges = ranges->count;
    _PPLRangeArray	*sorted = CreatePPLRangeArray(numRanges);
    unsigned		idx = 0;
    NSRange		range;
    /* We fill addresses and addressToLength */
    while (idx < numRanges) {
	/* we loop in increasing order because chances are that logged is sorted */
	range = ranges->ranges[idx];
	AddToPPLRangeArray(sorted, range, _PPLRangeArrayAddToHeap);
	idx++;
    }
    ranges->count = 0;
    while ( (range = RemoveMinimumOfPPLRangeArray(sorted, _PPLRangeArrayRemoveMinimumFromHeap)).length ) {
	/* length cannot be 0 */
	addAndTryCoalesceWithLast(ranges, range);
    }
    FreePPLRangeArray(sorted);
}

static void appendLogEntryToArray(LogEntry *entry, unsigned *nums, unsigned *count) {
    switch (entry->type) {
	case LogEntryMarker:
	    nums[(*count)++] = LOG_MARKER;
	    nums[(*count)++] = entry->address;
	    break;
	case LogEntryChange: {
	    unsigned	numVal = numValues(entry->address);
	    nums[(*count)++] = entry->address;
	    nums[(*count)++] = entry->num0;
	    if (numVal > 1) nums[(*count)++] = entry->num1;
	    if (numVal > 2) nums[(*count)++] = entry->num2;
	    if (numVal > 3) nums[(*count)++] = entry->num3;
	    break;
	}
	case LogEntryFree:
	    nums[(*count)++] = LOG_FREE_MARKER;
	    nums[(*count)++] = entry->address;
	    nums[(*count)++] = entry->num0;
	    break;
	default:
	    [NSException raise:NSInternalInconsistencyException format:@"*** appendLogEntryToArray"];
    }
}

/***************	Log Info	***************/

@implementation _PPLLogInfo

- initWithExistingLogForPath:(NSString *)path {
    logfd = [[NSFileHandle fileHandleForUpdatingAtPath:path] retain];
    if (logfd) {
	NS_DURING
	    lastLengthSyncedLog = [logfd seekToEndOfFile];
	NS_HANDLER
	    [self dealloc];
	    self = nil;
	NS_ENDHANDLER
    } else {
	[self dealloc];
	self = nil;
    }
    if (!self)
	PPLLog(@"*** Cannot open log for %@; Store is read only", path);
    else
	logged = CreatePPLRangeArray(0);
    return self;
}

- initAndRefreshLogForPath:(NSString *)path {
    NS_DURING
        int fd;
	_PPLMoveToBackup(path);
#if defined(WIN32)
        fd = open([path fileSystemRepresentation], O_RDWR | O_CREAT | O_TRUNC| _O_BINARY| _O_NOINHERIT, _S_IREAD | _S_IWRITE);
#else
        fd = open([path fileSystemRepresentation], O_RDWR | O_CREAT | O_TRUNC, 0666);
#endif
	if (0 <= fd)
	    logfd = [[NSFileHandle allocWithZone:NULL] initWithFileDescriptor:fd closeOnDealloc:YES];
    NS_HANDLER
	[self dealloc]; 
	self = nil;
    NS_ENDHANDLER
    if (!logfd) {
	[self dealloc]; 
	self = nil;
    }
    if (!self)
	PPLLog(@"*** Can't create log file %@", path);
    else
    	logged = CreatePPLRangeArray(0);
    return self;
}

- (void)dealloc {
    [logfd release];
    if (logged) FreePPLRangeArray(logged);
    [super dealloc];
}

- (BOOL)flushToBeLogged {
    /* flushes all changes not yet logged; 
    returns YES if log was written */
    if (numToBeLogged) {
	NSMutableData	*data;
	unsigned	nums[_PPLMaxLogged * 5];
	unsigned	count = 0;
	LogEntry	*entry = toBeLogged;
	while (numToBeLogged--) {
	    appendLogEntryToArray(entry, nums, &count);
	    if (entry->type != LogEntryMarker) {
		NSRange	range = logEntryRange(entry);
		approximateSinceLastCompute += range.length; // wrong because if can be less - or much more (a whole page!)
		/* we make an attempt to coalesce right away to decrease the amount of sorting to be done */
		addAndTryCoalesceWithLast(logged, range);
	    }
	    entry++;
	}
	numToBeLogged = 0; 
	data = [[NSMutableData allocWithZone:NULL] initWithCapacity:count * WORD_SIZE];
	[data serializeInts:nums count:count];
	[logfd writeData:data];
	[data release];
	return YES;
    } else {
	return NO;
    }
}

static BOOL appendLog(_PPLLogInfo *self, LogEntry entry) {
    BOOL	logWritten = NO;
    if (self->numToBeLogged >= _PPLMaxLogged) {
	logWritten = [self flushToBeLogged];
    }
    self->toBeLogged[self->numToBeLogged++] = entry;
    return logWritten;
}

- (BOOL)appendLogChangeAt:(unsigned)address original:(unsigned)original {
    /* we try to avoid logging the same address twice in a row */
    BOOL	logWritten = NO;
    LogEntry	entry = {LogEntryChange, address | 1, original, 0, 0, 0};
    LogEntry	*last = toBeLogged + numToBeLogged - 1;
    if (!numToBeLogged || (last->type != LogEntryChange) || !_tryOptimizeLog) {
	logWritten = appendLog(self, entry);
    } else {
	unsigned	previousAddress = last->address;
	unsigned	numVal = numValues(previousAddress);
	previousAddress = previousAddress & ~3U;
	if ((numVal != 4) && (address == previousAddress - WORD_SIZE)) {
	    //PPLLog(@"address 0x%x PRE previous 0x%x numVal %d", address, previousAddress, numVal);
	    last->address = address | ((numVal + 1) & 3);
	    last->num3 = last->num2;
	    last->num2 = last->num1;
	    last->num1 = last->num0;
	    last->num0 = original;
	} else if ((numVal != 4) && (address == previousAddress + WORD_SIZE * numVal)) {
	    //PPLLog(@"address 0x%x POST previous 0x%x numVal %d", address, previousAddress, numVal);
	    last->address = previousAddress | ((numVal + 1) & 3);
	    if (numVal == 1) last->num1 = original;
	    else if (numVal == 2) last->num2 = original;
	    else if (numVal == 3) last->num3 = original;
	} else if ((address < previousAddress) || (address >= previousAddress + numVal * WORD_SIZE)) {
	    logWritten = appendLog(self, entry);
	} else {
	    //PPLLog(@"address 0x%x covered by previous 0x%x numVal %d", address, previousAddress, numVal);
	}
    }
    return logWritten;
}

- (BOOL)appendLogChangesAt:(unsigned)address original4:(unsigned[4])original  {
    LogEntry	entry = {LogEntryChange, address, original[0], original[1], original[2], original[3]};
    return appendLog(self, entry);
}

- (BOOL)appendLogFreeAt:(unsigned)address length:(unsigned)length {
    LogEntry	entry = {LogEntryFree, address, length, 0, 0, 0};
    return appendLog(self, entry);
}

- (void)appendLogMarkerAndSync:(unsigned long long)lastLengthSyncedData {
    LogEntry	entry = {LogEntryMarker, (unsigned int)lastLengthSyncedData, 0, 0, 0, 0};
    appendLog(self, entry);
    [self flushToBeLogged];
    _PPLTestAndRaise(_PPLTestPostAppendingLogMarkerBeforeSync);
    [logfd synchronizeFile];
    _PPLTestAndRaise(_PPLTestPostAppendingLogMarkerAfterSync);
    lastLengthSyncedLog = [logfd offsetInFile];
    logged->count = 0;
}

- (_PPLRangeArray *)flushLogAndRangesOfChanges {
    _PPLRangeArray	*sorted = NULL;
    if (logged->count || numToBeLogged) {
	// this test is to force us to sync even if !numToBeLogged when some logging took place
	[self flushToBeLogged];
	_PPLTestAndRaise(_PPLTestPostAppendingLogChangesBeforeSync);
	[logfd synchronizeFile];
	_PPLTestAndRaise(_PPLTestPostAppendingLogChangesAfterSync);
    }
    if (numToBeLogged) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
    if (!logged->count) return NULL;
    /* we sort and coalesce to diminish disk operations */
    coalesceAndSortRanges(logged);
    sorted = logged;
    sizeOfChanged = 0;
    approximateSinceLastCompute = 0;
    logged = CreatePPLRangeArray(0);
    return sorted;
}

- (void)truncateLogAndMark:(unsigned long long)lastLengthSyncedData {
    [logfd truncateFileAtOffset:0ULL];
    [logfd seekToEndOfFile]; // needed: ftruncate does not change marker
    [self appendLogMarkerAndSync:lastLengthSyncedData];
}

+ (RecoveryType)recoverLogForPath:(NSString *)path applyToData:(NSMutableData *)current {
     RecoveryType	type = LogUnusable;
    NSData *logData = [[NSData allocWithZone:NULL] initWithContentsOfFile:path];
    if (!logData) {
        PPLLog(@"***Log %@ missing or inaccessible", path);
        return type;
    }
    NS_DURING
	unsigned	xlastLengthSyncedData = [current length];
	LogEntry	*entries = NULL;
	unsigned	numEntries = parseLogData(logData, &entries);
	if (!numEntries || (entries[0].type != LogEntryMarker)) {
	    PPLLog(@"*** Log %@ damaged: does not start with marker", path);
	} else if (entries[0].address > xlastLengthSyncedData) {
	    PPLLog(@"*** Log unusable or data file truncated; log cannot restore data file %@", path);
	} else if (numEntries == 1) {
	    if (entries[0].address == xlastLengthSyncedData) {
		type = LogNormal;
	    } else if (entries[0].address > xlastLengthSyncedData) {
		PPLLog(@"*** Log unusable or data file truncated; log cannot restore data file %@", path);
	    } else {
		[current setLength:entries[0].address];
		type = LogTruncated;
	    }
	} else {
	    NSData	*saved = [current copyWithZone:NULL];
	    PPLLog(@"*** Inconsistent PPL; Scavenging ...");
	    if (!scavenge(entries, numEntries, current)) {
		/* We may have started to modify current - oops */
		[current setLength:0];
		[current appendData:saved];
		type = LogUnsuccessfullyApplied;
	    } else {
		type = LogScavenged;
	    }
	    [saved release];
	}
	NSZoneFree(NSZoneFromPointer(entries), entries);
    NS_HANDLER
	PPLLog(@"*** Log %@ missing or can't be read", path);
    NS_ENDHANDLER
    [logData release];
    return type;	    
}

- (NSString *)description {
    if (!logged) return @"";
    return [NSString stringWithFormat:@"numLogged = %d; numToBeLogged = %d; log entries:\n%@", logged->count, numToBeLogged, stringForLogEntries(toBeLogged, numToBeLogged)];
}

- (unsigned)sizeOfAllPageChangedAndLogged {
    NSRange	pageRange;
    unsigned	idx = 1;
    if (!logged->count) return 0;
    if (approximateSinceLastCompute < sizeOfChanged) {
	/* we pretend to be happy with that estimate ... */
	return sizeOfChanged + approximateSinceLastCompute;
    }
    sizeOfChanged = 0;
    coalesceAndSortRanges(logged);
    pageRange = _PPLRoundUpRange(logged->ranges[0], NSPageSize());
    while (idx < logged->count) {
	NSRange	range = _PPLRoundUpRange(logged->ranges[idx], NSPageSize());
	if (pageRange.location > range.location) PPLLog(@"*** Inconsistency in sizeOfAllPageChangedAndLogged 1");
	if (pageRange.location + pageRange.length > range.location + range.length) PPLLog(@"*** Inconsistency in sizeOfAllPageChangedAndLogged 2");
	if (pageRange.location + pageRange.length >= range.location) {
	    /* we can coalesce */
	    pageRange.length = range.location + range.length - pageRange.location;
	} else {
	    sizeOfChanged += pageRange.length;
	    pageRange = range;
	}
	idx++;
    }
    sizeOfChanged += pageRange.length;
    approximateSinceLastCompute = 0;
    //PPLLog(@"sizeOfAllPageChangedAndLogged -> %d", sizeOfChanged);
    return sizeOfChanged;
}

@end

/***************	More Definitions	***************/

/* The enumerator is designed so that we can enumerate large dictionaries without loading all the keys at once; When a dictionary is going to be modified, its enumerators harden, i.e. really read all the keys */

@interface _PPLMutableDictEnum:NSEnumerator {
    @public
    _PPLMutableDict	*dict;
    PPLOrderedHeap	*allKeyHandles; // all handles to enumerate;
	// we use a heap in order to seriously improve VM access
    NSString *	*allKeys;	// NULL while the dictionary has not been modified; non-NULL => the truth!
    unsigned		numKeys;	// after hardening
}

- initWithDictionary:(_PPLMutableDict *)dict allKeyHandles:(PPLOrderedHeap *)allKeyHandles;
    /* transfer of ownership for allKeyHandles */

@end
    
static const unsigned preferedNumBuckets[] = {
    // given a current count (or less), given a 'good' numBuckets
    // that is at least one bigger (to avoid a nasty loop)
    // good numBuckets are prime and 3 * numBuckets + 3 is a multiple of 4 
    // (to maximize block use)
    2, 3,	// 75% use
    4, 7,	// 60%
    6, 11,	// 55%
    12, 23,	// 50%
    14, 31,	// 45%
    22, 67, 	// 30%
    43, 131,	// 30% ... thereafter
    87, 263,
    174, 523,
    343, 1031, 
    687, 2063, 
    1366, 4099, 
    2739, 8219, 
    5470, 16411, 
    10923, 32771, 
    21846, 65539, 
    43703, 131111, 
    87382, 262147, 
    174782, 524347, 
    349527, 1048583, 
    699070, 2097211, 
    1398106, 4194319, 
    2796206, 8388619, 
    5592419, 16777259, 
    11184822, 33554467, 
    22369626, 67108879, 
    44739259, 134217779, 
    89478486, 268435459, 
    178956974, 536870923, 
    357913942, 1073741827, 
    UINT_MAX, 2147483659U
};

// constants
#define MAX_PERCENT		35	// when more than that % use, force a rehash!

// Convenient macro...
static void zombieError(id self, SEL _cmd) {
    [NSException raise:NSInvalidArgumentException format:@"Cannot perform -%@ on a PPL proxy 0x%x after -detachFromFile has been called!", NSStringFromSelector(_cmd), self];
}

/***************	Private PPL defs	***************/

@interface PPL (Private)

- (void)_setDirty;

- (unsigned)_store:value;

- (unsigned)_noCreateHandleForValue:value;

- (void)_decrRefCountInStoreFor:(unsigned)handle;

- (void)_decrRefCountInStoreMultiple:(PPLOrderedHeap *)handles;

@end

/***************	Utilities	***************/

static void _PPLBug(PPL *self, SEL _cmd) {
    [self _raiseFormat:@"*** PPL exception in %@", NSStringFromSelector(_cmd)];
}

static void makeForwardBlock(_PPLStore *store, unsigned handle, unsigned newHandle) {
    BlockHeader	header = [store readHeaderInBlock:handle];
    header.type = STORED_FORWARD;
    if (header.capacity < WORD_SIZE) header.capacity = WORD_SIZE;
    /* we want the capacity to be at least 4 so that it can hold newHandle;
    but we leave it alone if big enough */
    [store writeHeader:header inBlock:handle];
    [store writeUnsigned:newHandle inBlock:handle atIndex:0];
}

static NSString *retrieveFirstStringInHeap(PPL *self, PPLOrderedHeap *handles) {
    while (PPLCountOrderedHeap(handles)) {
	unsigned 	handle = (unsigned)PPLRemoveMinimumOfOrderedHeap(handles);
	NSString	*value = [[self _cache] cachedObjectForHandle:handle];
	unsigned	type;
	_PPLStore 	*store = [self _store];
	if (value) return value;
	type = [store readHeaderInBlock:handle].type;
	switch (type) {
	    case STORED_NSSTRING:
		return [store readNEXTSTEPStringInBlock:handle notifyCache:YES];
	    case STORED_FORWARD:
		handle = [store unsignedForBlock:handle atIndex:0];
		PPLAddToOrderedHeap(handles, (void *)handle);
		break;
	    default:
		[self _raiseFormat:@"*** PPL retrieving an enumerator key: unknown ref code: %d", type];
		return nil;
	}
    }
    return nil;
}

/***************	Dictionary and array extras	***************/

@interface NSMutableArray (NSMutableArrayPPL)

- (void)_freezeForHandle:(unsigned)handle inPPL:(PPL *)ppl;

@end

@implementation NSMutableArray (NSMutableArrayPPL)

- (void)_freezeForHandle:(unsigned)mhandle inPPL:(PPL *)ppl {
    _PPLStore	*store = [ppl _store];
    NSMutableArray	*pplArray;
    unsigned		type;
    mhandle = _PPLFollowForwardingHandles(ppl, mhandle);
    type = [store readHeaderInBlock:mhandle].type;
    if (type != STORED_MARRAY) _PPLBug(ppl, _cmd);
    pplArray = [[_PPLMutableArray allocWithZone:NULL] initWithPPL:ppl forHandle:mhandle]; 
    [pplArray setArray:self];
    [pplArray release];
}

@end

@interface NSMutableDictionary (NSMutableDictionaryPPL)

- (void)_freezeForHandle:(unsigned)handle inPPL:(PPL *)ppl;

@end

@implementation NSMutableDictionary (NSMutableDictionaryPPL)

- (void)_freezeForHandle:(unsigned)mhandle inPPL:(PPL *)ppl {
    _PPLStore	*store = [ppl _store];
    NSMutableDictionary	*pplDict;
    unsigned		type;
    mhandle = _PPLFollowForwardingHandles(ppl, mhandle);
    type = [store readHeaderInBlock:mhandle].type;
    if (type != STORED_MDICT) _PPLBug(ppl, _cmd);
    pplDict = [[_PPLMutableDict allocWithZone:NULL] initWithPPL:ppl forHandle:mhandle];
    [pplDict setDictionary:self];
    [pplDict release];
}

@end

/***************	Mutable Array proxy	***************/

@interface NSArray (PPLGoodies)
- (id)initWithArray:(NSArray *)array copyItems:(BOOL)flag;
@end

@implementation NSArray (PPLGoodies)
- (id)initWithArray:(NSArray *)array copyItems:(BOOL)flag {
    unsigned idx, cnt = [array count];
    NSZone *zone = flag ? NSZoneFromPointer(self) : NULL;
    id *list, buffer[128];
    if (0 == cnt) return [self initWithObjects:NULL count:0];
    list = (cnt <= 128) ? buffer : NSZoneMalloc(NULL, cnt * sizeof(id));
    [array getObjects:list range:NSMakeRange(0, cnt)];
    if (flag)
        for (idx = 0; idx < cnt; idx++)
            list[idx] = [list[idx] copyWithZone:zone];
    self = [self initWithObjects:list count:cnt];
    if (flag)
        for (idx = 0; idx < cnt; idx++)
            [list[idx] release];
    if (list != buffer) NSZoneFree(NULL, list);
    return self;
}
@end

@implementation _PPLMutableArray

+ (unsigned)createArrayBlockInStore:(_PPLStore *)store max:(unsigned *)maxi {
    /* We store arrays as the ref count, type, the number of slots in the table, the number of items, and then the slots themselves;
    max passed in is a minimum; true max is returned */
    unsigned		capacity = ((*maxi) + 1) * WORD_SIZE; /* in bytes */
    BlockHeader	header = {1, STORED_MARRAY, capacity};
    NSMutableData	*mdata = [[NSMutableData allocWithZone:NULL] initWithCapacity:capacity];
    unsigned		ahandle;
    unsigned		trueCapacity;
    [mdata setLength:capacity]; /* mdata is zero-filled */
    ahandle = [store createBlockForHeader:header andData:mdata reuseFreeBlock:YES cold:NO trueCapacity:&trueCapacity];
    [mdata release];
    if (capacity != trueCapacity) {
	header.capacity = trueCapacity;
	[store writeHeader:header inBlock:ahandle];
    }
    return ahandle;
}

- initWithPPL:(PPL *)appl forHandle:(unsigned)hh {
    _PPLStore	*store = [appl _store];
    BlockHeader	header = [store readHeaderInBlock:hh];
    ppl = appl;
    handle = hh;
    max = header.capacity / WORD_SIZE - 1;
    count = [store unsignedForBlock:hh atIndex:0];
    return self;
}

- (unsigned)_handle {
    return handle;
}

- (PPL *)_ppl {
    return ppl;
}

- (void)_freezeForHandle:(unsigned)mhandle inPPL:(PPL *)appl {
    if (!ppl) PPLLog(@"*** zombie encountered in _freezeForHandle:inPPL:!");
    if (appl == ppl) {
	/* Already frozen; note that mhandle and handle can be different since one can be forward to the other */
	return;
    } else {
	[super _freezeForHandle:mhandle inPPL:appl];
    }
}

- (void)_zombifyInPPL:(PPL *)appl {
    if (!ppl) PPLLog(@"*** zombie encountered in _zombifyInPPL:!");
    if (ppl != appl) return; /* This mutable belongs to 2 or more PPL */
    ppl = nil;
}

- retain {
    _PPLIncrementInternalRefCount(self, &inMemoryRefcount, UINT_MAX, nil);
    return self;
}

- (void)release {
    if (_PPLDecrementInternalRefCountWasZero(self, &inMemoryRefcount, UINT_MAX, nil))
	[self dealloc]; 
}

/* Note that contrary to the norm, this method is not just a debug help;
it is used to decide which mutable objects to forget after -save or -flush */
- (unsigned)retainCount {
    return _PPLInternalRefCount(self, inMemoryRefcount, UINT_MAX);
}

- (unsigned)_inStoreRefCount {
    return [[ppl _store] readHeaderInBlock:handle].refCount;
}

- (void)_destroySubsInStorage {
    if (!ppl) PPLLog(@"*** zombie encountered in _destroySubsInStorage!");
    [self removeAllObjects];
    ppl = nil; // as a safety measure in case someone was trying to do anything other than release to self
}

- (unsigned)count {
    return count;
}

- objectAtIndex:(unsigned)idx {
    if (!ppl) zombieError(self, _cmd);
    if (idx >= count) [NSException raise:NSRangeException format:@"*** -%@ %d is beyond count (%d)", NSStringFromSelector(_cmd), idx, count];
    return _PPLRetrieveCompact(ppl, handle, idx + 1, YES);
}

- (void)_grow {
    _PPLStore	*store = [ppl _store];
    unsigned	newHandle;
    unsigned	idx = 0;
    max += max + 1;
    newHandle = [_PPLMutableArray createArrayBlockInStore:store max:&max];
    /* max is now at least twice of what it was! */
    while (idx < count) {
	unsigned	sub = [store unsignedForBlock:handle atIndex:idx + 1];
	[store writeUnsigned:sub inBlock:newHandle atIndex:idx + 1];
	[store writeUnsigned:0 inBlock:handle atIndex:idx + 1]; /* laundry */
	idx++;
    }
    makeForwardBlock(store, handle, newHandle);
    handle = newHandle;
}

- (void)addObject:anObject {
    _PPLStore	*store = [ppl _store];
    unsigned	sub;
    if (!ppl) zombieError(self, _cmd);
    if (!anObject) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil object", NSStringFromSelector(_cmd)];
    [ppl _setDirty];
    sub = [ppl _store:anObject];
    /* we store before growing to keep the immutable objects ahead */
    if (count + 1 > max) [self _grow];
    count++;
    [store writeUnsigned:sub inBlock:handle atIndex:count];
    [store writeUnsigned:count inBlock:handle atIndex:0];
}

- (void)replaceObjectAtIndex:(unsigned)idx withObject:newObject {
    _PPLStore	*store = [ppl _store];
    unsigned	old;
    unsigned	newHandle;
    if (!ppl) zombieError(self, _cmd);
    if (idx >= count) [NSException raise:NSRangeException format:@"*** %@ %d is beyond count (%d)", NSStringFromSelector(_cmd), idx, count];
    if (!newObject) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil object", NSStringFromSelector(_cmd)];
    old = [store unsignedForBlock:handle atIndex:idx + 1];
    newHandle = [ppl _noCreateHandleForValue:newObject];
    if (newHandle && (old == newHandle)) return; /* no change */
    [ppl _setDirty];
    newHandle = [ppl _store:newObject];
    /* It is important to store new before decrementing the old ref count! */
    [store writeUnsigned:newHandle inBlock:handle atIndex:idx + 1];
    [ppl _decrRefCountInStoreFor:old];
}

- (void)removeLastObject {
    _PPLStore	*store = [ppl _store];
    unsigned	sub;
    if (!ppl) zombieError(self, _cmd);
    if (!count) [NSException raise:NSRangeException format:@"*** %@ empty array", NSStringFromSelector(_cmd)];
    [ppl _setDirty];
    sub = [store unsignedForBlock:handle atIndex:count];
    [ppl _decrRefCountInStoreFor:sub];
    count--;
    [store writeUnsigned:count inBlock:handle atIndex:0];
    [store writeUnsigned:0 inBlock:handle atIndex:count + 1]; /* laundry that location */
}

- (void)insertObject:object atIndex:(unsigned)idx {
    _PPLStore	*store = [ppl _store];
    unsigned	sub;
    if (!ppl) zombieError(self, _cmd);
    if (!object) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil object", NSStringFromSelector(_cmd)];
    if (idx > count) [NSException raise:NSRangeException format:@"*** %@ idx (%d) beyond bounds (%d)", NSStringFromSelector(_cmd), idx, count];
    [ppl _setDirty];
    sub = [ppl _store:object];
    /* we store before growing to keep the immutable objects ahead */
    if (count + 1 > max) [self _grow];
    while (idx < count) {
	unsigned	previous = [store unsignedForBlock:handle atIndex:idx + 1];
	[store writeUnsigned:sub inBlock:handle atIndex:idx + 1];
	sub = previous;
	idx++;
    }
    count++;
    [store writeUnsigned:sub inBlock:handle atIndex:count];
    [store writeUnsigned:count inBlock:handle atIndex:0];
}

- (void)removeObjectAtIndex:(unsigned)idx {
    _PPLStore	*store = [ppl _store];
    unsigned	sub;
    if (!ppl) zombieError(self, _cmd);
    if (idx >= count) [NSException raise:NSRangeException format:@"*** %@ index (%d) beyond bounds (%d)", NSStringFromSelector(_cmd), idx, count];
    [ppl _setDirty];
    sub = [store unsignedForBlock:handle atIndex:idx + 1];
    [ppl _decrRefCountInStoreFor:sub];
    [store downShiftUnsigned:NSMakeRange(idx+1+1, count - idx - 1) inBlock:handle by:1];
    count--;
    [store writeUnsigned:0 inBlock:handle atIndex:count + 1]; /* laundry */
    [store writeUnsigned:count inBlock:handle atIndex:0];
}

- (void)removeObjectsFromIndices:(unsigned *)indices numIndices:(unsigned)numIndices {
    // we redefine this method for performance
    _PPLStore		*store = [ppl _store];
    PPLOrderedHeap	*sindices; // we sort indices to avoid being N*2 in movement of information
    PPLOrderedHeap	*subs; // we sort handles for which to decrease the refcount in order to improve disk access
    unsigned		sub;
    unsigned		idx = 0;
    unsigned		shift = 0;
    if (!ppl) zombieError(self, _cmd);
    if (!numIndices) return;
    [ppl _setDirty];
    sindices = PPLCreateOrderedHeap(NULL, numIndices);
    while (idx < numIndices) {
	unsigned	ix = indices[idx];
	if (ix >= count) [NSException raise:NSRangeException format:@"*** %@ index (%d) beyond bounds (%d)", NSStringFromSelector(_cmd), ix, count];
	PPLAddToOrderedHeap(sindices, (void *)ix);
	idx++;
    }
    subs = PPLCreateOrderedHeap(NULL, numIndices);
    idx = (unsigned)PPLRemoveMinimumOfOrderedHeap(sindices);
    sub = [store unsignedForBlock:handle atIndex:idx + 1];
    PPLAddToOrderedHeap(subs, (void *)sub);
    shift++;
    // at the entrance of this loop idx is the previous index removed
    while (PPLCountOrderedHeap(sindices)) {
	unsigned	newIndex = (unsigned)PPLRemoveMinimumOfOrderedHeap(sindices);
	if (newIndex != idx) {
	    sub = [store unsignedForBlock:handle atIndex:newIndex + 1];
	    PPLAddToOrderedHeap(subs, (void *)sub);
	    // we shift all the locations between idx and newIndex (excluded)
	    [store downShiftUnsigned:NSMakeRange(idx+1+1, newIndex-idx-1) inBlock:handle by:shift];
	    shift++;
	    idx = newIndex;
	}
    }
    // we have to shift all the ones between idx and count (excluded)
    [store downShiftUnsigned:NSMakeRange(idx+1+1, count-idx-1) inBlock:handle by:shift];
    PPLFreeOrderedHeap(sindices);
    [ppl _decrRefCountInStoreMultiple:subs];
    PPLFreeOrderedHeap(subs);
    idx = shift;
    while (idx--) {
	count--;
	[store writeUnsigned:0 inBlock:handle atIndex:count + 1]; /* laundry */
    }
    [store writeUnsigned:count inBlock:handle atIndex:0];
}

- (void)removeAllObjects {
    /* Speed up */
    _PPLStore	*store = [ppl _store];
    unsigned	idx = 0;
    PPLOrderedHeap	*handles; // we sort handles for which to _decrRefCountInStoreFor: in order to improve locality
    if (!count) return;
    if (!ppl) zombieError(self, _cmd);
    [ppl _setDirty];
    handles = PPLCreateOrderedHeap(NULL, count);
    while (idx < count) {
	unsigned	sub;
	sub = [store unsignedForBlock:handle atIndex:idx+1];
	PPLAddToOrderedHeap(handles, (void *)sub);
	[store writeUnsigned:0 inBlock:handle atIndex:idx+1]; /* laundry that location */
	idx++;
    }
    count = 0;
    [store writeUnsigned:count inBlock:handle atIndex:0];
    [ppl _decrRefCountInStoreMultiple:handles];
    PPLFreeOrderedHeap(handles);
}

- (id)copyWithZone:(NSZone *)zone {
    if (!ppl) zombieError(self, _cmd);
    return [[NSArray allocWithZone:zone] initWithArray:self copyItems:YES];
} 

- (id)mutableCopyWithZone:(NSZone *)zone {
    if (!ppl) zombieError(self, _cmd);
    return [[NSMutableArray allocWithZone:zone] initWithArray:self];
} 

- (BOOL)isEqual:(id)other {
    if (!ppl) zombieError(self, _cmd);
    return (other == (id)self) || ([other isKindOfClass:[NSArray class]] && [self isEqualToArray:other]);
}

- (unsigned)hash {
    return count;
}

@end

/***************	Mutable Dictionary Enumerator	***************/

/* We have enumerators so that users can enumerate large dictionaries without loading all the keys at once; When a dictionary is going to be modified, its enumerators harden, i.e. really read all the keys */

@implementation _PPLMutableDictEnum

- initWithDictionary:(_PPLMutableDict *)dd allKeyHandles:(PPLOrderedHeap *)akh;
 {
    dict = [dd retain];
    allKeyHandles = akh;
    return self;
}

- nextObject {
    if (allKeys) {
	if (!numKeys) {
	    [dict _removeEnumerator:self]; // note that this may release dict
	    return nil; // make sure always returns nil if called repeatedly
	}
	return [allKeys[--numKeys] autorelease];
    } else if (PPLCountOrderedHeap(allKeyHandles)) {
	return retrieveFirstStringInHeap([dict _ppl], allKeyHandles);
    } else {
	[dict _removeEnumerator:self]; // note that this may release dict
	return nil; // make sure always returns nil if called repeatedly
    }
}

- (void)dealloc {
    [dict _removeEnumerator:self]; // note that this may release dict
    if (allKeys) {
	while (numKeys) [allKeys[--numKeys] release];
	NSZoneFree(NSZoneFromPointer(allKeys), allKeys);
    }
    PPLFreeOrderedHeap(allKeyHandles);
    [super dealloc];
}

@end
    
/***************	Mutable Dictionary proxy	***************/

/* The old hash functions used by NSString. We put them here because they are now used only by PPL.
*/
#define MAXSTRINGLENFORHASHING 63
   

static const struct _charmap {
    unichar _u;
    unsigned char _c;
} hash_char_map[128] = {
        { 0x00a0, 0x80 },
        { 0x00a1, 0xa1 },
        { 0x00a2, 0xa2 },
        { 0x00a3, 0xa3 },
        { 0x00a4, 0xa8 },
        { 0x00a5, 0xa5 },
        { 0x00a6, 0xb5 },
        { 0x00a7, 0xa7 },
        { 0x00a8, 0xc8 },
        { 0x00a9, 0xa0 },
        { 0x00aa, 0xe3 },
        { 0x00ab, 0xab },
        { 0x00ac, 0xbe },
        { 0x00ae, 0xb0 },
        { 0x00af, 0xc5 },
        { 0x00b1, 0xd1 },
        { 0x00b2, 0xc9 },
        { 0x00b3, 0xcc },
        { 0x00b4, 0xc2 },
        { 0x00b5, 0x9d },
        { 0x00b6, 0xb6 },
        { 0x00b7, 0xb4 },
        { 0x00b8, 0xcb },
        { 0x00b9, 0xc0 },
        { 0x00ba, 0xeb },
        { 0x00bb, 0xbb },
        { 0x00bc, 0xd2 },
        { 0x00bd, 0xd3 },
        { 0x00be, 0xd4 },
        { 0x00bf, 0xbf },
        { 0x00c0, 0x81 },
        { 0x00c1, 0x82 },
        { 0x00c2, 0x83 },
        { 0x00c3, 0x84 },
        { 0x00c4, 0x85 },
        { 0x00c5, 0x86 },
        { 0x00c6, 0xe1 },
        { 0x00c7, 0x87 },
        { 0x00c8, 0x88 },
        { 0x00c9, 0x89 },
        { 0x00ca, 0x8a },
        { 0x00cb, 0x8b },
        { 0x00cc, 0x8c },
        { 0x00cd, 0x8d },
        { 0x00ce, 0x8e },
        { 0x00cf, 0x8f },
        { 0x00d0, 0x90 },
        { 0x00d1, 0x91 },
        { 0x00d2, 0x92 },
        { 0x00d3, 0x93 },
        { 0x00d4, 0x94 },
        { 0x00d5, 0x95 },
        { 0x00d6, 0x96 },
        { 0x00d7, 0x9e },
        { 0x00d8, 0xe9 },
        { 0x00d9, 0x97 },
        { 0x00da, 0x98 },
        { 0x00db, 0x99 },
        { 0x00dc, 0x9a },
        { 0x00dd, 0x9b },
        { 0x00de, 0x9c },
        { 0x00df, 0xfb },
        { 0x00e0, 0xd5 },
        { 0x00e1, 0xd6 },
        { 0x00e2, 0xd7 },
        { 0x00e3, 0xd8 },
        { 0x00e4, 0xd9 },
        { 0x00e5, 0xda },
        { 0x00e6, 0xf1 },
        { 0x00e7, 0xdb },
        { 0x00e8, 0xdc },
        { 0x00e9, 0xdd },
        { 0x00ea, 0xde },
        { 0x00eb, 0xdf },
        { 0x00ec, 0xe0 },
        { 0x00ed, 0xe2 },
        { 0x00ee, 0xe4 },
        { 0x00ef, 0xe5 },
        { 0x00f0, 0xe6 },
        { 0x00f1, 0xe7 },
        { 0x00f2, 0xec },
        { 0x00f3, 0xed },
        { 0x00f4, 0xee },
        { 0x00f5, 0xef },
        { 0x00f6, 0xf0 },
        { 0x00f7, 0x9f },
        { 0x00f8, 0xf9 },
        { 0x00f9, 0xf2 },
        { 0x00fa, 0xf3 },
        { 0x00fb, 0xf4 },
        { 0x00fc, 0xf6 },
        { 0x00fd, 0xf7 },
        { 0x00fe, 0xfc },
        { 0x00ff, 0xfd },
        { 0x0131, 0xf5 },
        { 0x0141, 0xe8 },
        { 0x0142, 0xf8 },
        { 0x0152, 0xea },
        { 0x0153, 0xfa },
        { 0x0192, 0xa6 },
        { 0x02c6, 0xc3 },
        { 0x02c7, 0xcf },
        { 0x02cb, 0xc1 },
        { 0x02d8, 0xc6 },
        { 0x02d9, 0xc7 },
        { 0x02da, 0xca },
        { 0x02db, 0xce },
        { 0x02dc, 0xc4 },
        { 0x02dd, 0xcd },
        { 0x2013, 0xb1 },
        { 0x2014, 0xd0 },
        { 0x2019, 0xa9 },
        { 0x201a, 0xb8 },
        { 0x201c, 0xaa },
        { 0x201d, 0xba },
        { 0x201e, 0xb9 },
        { 0x2020, 0xb2 },
        { 0x2021, 0xb3 },
        { 0x2022, 0xb7 },
        { 0x2026, 0xbc },
        { 0x2029, 0x0a },
        { 0x2030, 0xbd },
        { 0x2039, 0xac },
        { 0x203a, 0xad },
        { 0x2044, 0xa4 },
        { 0xfb01, 0xae },
        { 0xfb02, 0xaf },
        { 0xfffd, 0xff },
};

static BOOL unicode2ns(unichar aChar, char *ch) {
    if (aChar < 128) {
        *ch = aChar;
        return YES;
    } else {
        const struct _charmap *p, *q, *divider;
        if ((aChar < hash_char_map [0]._u) || (aChar > hash_char_map [128-1]._u)) {
            return NO;
        }
        p = hash_char_map;
        q = p + (128-1);
        while (p <= q) {
            divider = p + ((q - p) >> 1);	/* divide by 2 */
            if (aChar < divider->_u) { q = divider - 1; }
            else if (aChar > divider->_u) { p = divider + 1; }
            else { *ch = divider->_c; return YES; }
        }
    }
    return NO;
}

static unsigned _NSOldHashCharacters(const unichar *characters, unsigned length) {
    unsigned int h = length, cnt;
    if (length > MAXSTRINGLENFORHASHING) length = MAXSTRINGLENFORHASHING;
    for (cnt = 0; cnt < length; cnt++) {
        unichar ch = characters[cnt];
        h <<= 4;
        if (ch < 128) {
            h += ch;
        } else {
            unsigned char nsChar;	/* ??? We just do the precomps for now; bogus. We also cast this to unsigned */
            if (unicode2ns(ch, &nsChar)) h += (unsigned int)nsChar;
        }
        h ^= (h >> 24);
    }
    return h;
}

static unsigned HASH_STR(NSString *str) {
    unsigned	hash = 0;
    {
	unsigned	length = [str length];
	unsigned	lengthToGet = (length > MAXSTRINGLENFORHASHING) ? MAXSTRINGLENFORHASHING : length;
        unichar		buffer[MAXSTRINGLENFORHASHING];
	[str getCharacters:buffer range:NSMakeRange(0, lengthToGet)];
	hash = _NSOldHashCharacters(buffer, length);
    }
    return (hash << 1) + 1;	// never 0 !!
}

#define HASH(idx)	[store unsignedForBlock:self->handle atIndex:idx * 3 + 1]
#define KEY(idx)	[store unsignedForBlock:self->handle atIndex:idx * 3 + 2]
#define VALUE(idx)	[store unsignedForBlock:self->handle atIndex:idx * 3 + 3]

#define SET_HASH(hash,idx)	   [store writeUnsigned:hash inBlock:handle atIndex:idx * 3 + 1]
#define SET_KEY(keyHandle,idx)   [store writeUnsigned:keyHandle inBlock:handle atIndex:idx * 3 + 2]
#define SET_VALUE(valHandle,idx) [store writeUnsigned:valHandle inBlock:handle atIndex:idx * 3 + 3]

static unsigned bestNumBuckets(unsigned countToHold) {
    const unsigned	*possibleNumBuckets = preferedNumBuckets;
    while (countToHold > possibleNumBuckets[0]) possibleNumBuckets += 2;
    if (possibleNumBuckets[1] < countToHold) PPLLog(@"_rehash: error");
    return possibleNumBuckets[1];
}

static unsigned capacityForBuckets(unsigned numBuckets) {
    return (numBuckets * 3 + 1) * WORD_SIZE;
}

static unsigned bucketsForCapacity(PPL *ppl, unsigned capacity) {
    if ((capacity - WORD_SIZE) % (3 * WORD_SIZE)) {
	if (ppl) [ppl _raiseFormat:@"*** Damaged dictionary: %d", capacity];
	else [NSException raise:NSInternalInconsistencyException format:@"*** Damaged dictionary: %d", capacity];
    }
    return (capacity - WORD_SIZE) / (3 * WORD_SIZE);
}

@implementation _PPLMutableDict

- initWithPPL:(PPL *)appl forHandle:(unsigned)hh {
    _PPLStore		*store = [appl _store];
    BlockHeader	header = [store readHeaderInBlock:hh];
    ppl = appl;
    handle = hh;
    numBuckets = bucketsForCapacity(ppl, header.capacity);
    count = [store unsignedForBlock:hh atIndex:0];
    if (!numBuckets) [ppl _raiseFormat:@"*** %@ no buckets", NSStringFromSelector(_cmd)];
    if (count > numBuckets) [ppl _raiseFormat:@"*** %@ count too large", NSStringFromSelector(_cmd)];
    return self;
}

static unsigned createDictBlock(_PPLStore *store, unsigned numBuckets) {
    /* We store dictionaries as the ref count, type, the number of pair slots in the table, the number of pairs, and then the slots themselves */
    unsigned		capacity = capacityForBuckets(numBuckets);
    BlockHeader	header = {1, STORED_MDICT, capacity};
    NSMutableData	*mdata = [[NSMutableData allocWithZone:NULL] initWithCapacity:capacity];
    unsigned		dhandle;
    [mdata setLength:capacity]; /* mdata is zero-filled */
    dhandle = [store createBlockForHeader:header andData:mdata reuseFreeBlock:YES cold:NO trueCapacity:NULL];
    /* we do not want to change numBuckets, so we don't use all the bytes as indicated by trueCapacity */
    [mdata release];
    return dhandle;
}

+ (unsigned)createDictBlockInStore:(_PPLStore *)store capacity:(unsigned) capacity {
    return createDictBlock(store, bestNumBuckets(capacity));
}

- (unsigned)_handle {
    return handle;
}

- (PPL *)_ppl {
    return ppl;
}

- (void)_destroySubsInStorage {
    if (!ppl) PPLLog(@"*** zombie encountered in _destroySubsInStorage!");
    [self _hardenEnumerators];
    [self removeAllObjects];
    ppl = nil; // as a safety measure in case someone was trying to do anything other than release to self
}

- (void)_freezeForHandle:(unsigned)mhandle inPPL:(PPL *)appl {
    if (!ppl) PPLLog(@"*** zombie encountered in _freezeForHandle:inPPL:!");
    if (appl == ppl) {
	/* Already frozen; note that mhandle and handle can be different since one can be forward to the other */
	return;
    } else {
	[super _freezeForHandle:mhandle inPPL:appl];
    }
}

- (void)_zombifyInPPL:(PPL *)appl {
    if (!ppl) PPLLog(@"*** zombie encountered in _zombifyInPPL:!");
    if (ppl != appl) return; /* This mutable belongs to 2 or more PPL */
    [self _hardenEnumerators];
    NSZoneFree(NSZoneFromPointer(enumerators), enumerators);
    enumerators = NULL;
    ppl = nil;
}

- retain {
    inMemoryRefcount++;
    return self;
}

- (unsigned)retainCount {
    unsigned	res;
    /* Note that contrary to the norm, this method is not just a debug help;
    it is used to decide which mutable objects to forget after -save or -flush */
    res = inMemoryRefcount + 1;
    return res;
}

- (void)release {
    if (inMemoryRefcount) {
	inMemoryRefcount--;
	return;
    }
    [self dealloc]; 
}

- (unsigned)_inStoreRefCount {
    return [[ppl _store] readHeaderInBlock:handle].refCount;
}

- (void)dealloc {
    [self _hardenEnumerators];
    NSZoneFree(NSZoneFromPointer(enumerators), enumerators);
    enumerators = NULL;
    [super dealloc];
}

static NSString *keyAtIndex(_PPLMutableDict *self, unsigned idx) {
    _PPLStore		*store = [self->ppl _store];
    unsigned		keyHandle = KEY(idx);
    NSString 		*key;
    if (!keyHandle) return nil;
    key = _PPLRetrieveCompact(self->ppl, self->handle, idx * 3 + 2, NO);
    if (HASH_STR(key) != HASH(idx)) {
	[self->ppl _raiseFormat:@"*** Inconsistent dictionary: stored key is '%@' hashes to 0x%x while hash value in store is 0x%x", key, HASH_STR(key), HASH(idx)];
    }
    return key;
}

- (void)_zapAtIndex:(unsigned)idx {
    _PPLStore	*store = [ppl _store];
    SET_HASH(0, idx);
    SET_KEY(0, idx);
    SET_VALUE(0, idx);
}

- (void)_readHashKeyValueInto:(unsigned *)hkv atIndex:(unsigned)idx {
    _PPLStore	*store = [ppl _store];
    *hkv = HASH(idx);
    hkv++;
    *hkv = KEY(idx);
    hkv++;
    *hkv = VALUE(idx);
}

- (void)_reInsertPairs:(unsigned)num handles:(unsigned *)oldHandles {
    _PPLStore	*store = [ppl _store];
    while (num--) {
	unsigned	hash = oldHandles[num*3];
	unsigned	keyHandle = oldHandles[num*3+1];
	unsigned	valueHandle = oldHandles[num*3+2];
	if (keyHandle) {
	    unsigned	idx = hash % numBuckets;
	    while (1) {
		unsigned	bkeyHandle = KEY(idx);
		if (!bkeyHandle) {
		    SET_HASH(hash, idx);
		    SET_KEY(keyHandle, idx);
		    SET_VALUE(valueHandle, idx);
		    break;
		}
		idx++;
		if (idx >= numBuckets) idx -= numBuckets;
	    }
	} 
    }
}

- (void)_rehash {
    _PPLStore	*store = [ppl _store];
    unsigned	idx = numBuckets;
    unsigned	*oldHandles = NSZoneMalloc(NULL, numBuckets * 3 * WORD_SIZE);
    unsigned	oldNumBuckets;
    unsigned	newHandle;
    /* we make sure oldHandles contains everything */
    while (idx--) {
	[self _readHashKeyValueInto:oldHandles+(idx*3) atIndex:idx];
	[self _zapAtIndex:idx];
    }
    /* we resize the buckets, and fill with nullSpec */
    oldNumBuckets = numBuckets;
    numBuckets = bestNumBuckets(count+1);
    newHandle = createDictBlock(store, numBuckets);
    makeForwardBlock(store, handle, newHandle);
    handle = newHandle;
    /* we replenish */
    [self _reInsertPairs:oldNumBuckets handles:oldHandles];
    NSZoneFree(NSZoneFromPointer(oldHandles), oldHandles);
    [store writeUnsigned:count inBlock:handle atIndex:0];
}

- (unsigned)count {
    return count;
}

- objectForKey:(NSString *)key {
    _PPLStore	*store = [ppl _store];
    unsigned	hash = HASH_STR(key);
    unsigned	idx = hash % numBuckets;
    id		value = nil;
    if (!ppl) zombieError(self, _cmd);
    if (!key) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil key", NSStringFromSelector(_cmd)];
    if (![key isKindOfClass:[NSString class]]) [NSException raise:NSInvalidArgumentException format:@"*** %@ key not a string:'%@'", NSStringFromSelector(_cmd), key]; // see bug #38650
    while (1) {
	unsigned	keyHandle = KEY(idx);
	if (!keyHandle) break;
	if ((HASH(idx) == hash) && [keyAtIndex(self, idx) isEqualToString:key]) {
	    /* we are careful to not create the key if it is the wrong hash */
	    unsigned	valueHandle = VALUE(idx);
	    if (!valueHandle) _PPLBug(ppl, _cmd);
	    value = _PPLRetrieveCompact(ppl, handle, idx * 3 + 3, YES);
	    break;
	}
	idx++;
	if (idx >= numBuckets) idx -= numBuckets;
    }
    return value;
}

- (void)setObject:value forKey:(NSString *)key {
    _PPLStore	*store = [ppl _store];
    unsigned	hash = HASH_STR(key);
    unsigned	idx = hash % numBuckets;
    if (!ppl) zombieError(self, _cmd);
    if (!key) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil key", NSStringFromSelector(_cmd)];
    if (![key isKindOfClass:[NSString class]]) [NSException raise:NSInvalidArgumentException format:@"*** %@ key not a string:'%@'", NSStringFromSelector(_cmd), key]; // see bug #38650
    if (!value) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil value", NSStringFromSelector(_cmd)];
    [ppl _setDirty];
    [self _hardenEnumerators];
    if (count + 1 >= numBuckets) {
	/* ensure we never have count==numBuckets (loop forever) */
	[self _rehash];
	[self setObject:value forKey:key];
	return;
    }
    while (1) {
	unsigned keyHandle = KEY(idx);
	if (!keyHandle) {
	    unsigned valueHandle;
	    keyHandle = [ppl _store:key];
	    valueHandle = [ppl _store:value];
	    SET_HASH(hash, idx);
	    SET_KEY(keyHandle, idx);
	    SET_VALUE(valueHandle, idx);
	    count++;
	    [store writeUnsigned:count inBlock:handle atIndex:0];
	    if (count * 100 > numBuckets * MAX_PERCENT) [self _rehash];
	    break;
	}
	if ((HASH(idx) == hash) && [keyAtIndex(self, idx) isEqualToString:key]) {
	    unsigned	oldValueHandle = VALUE(idx);
	    unsigned	newValueHandle = [ppl _noCreateHandleForValue:value];
	    if (newValueHandle && (oldValueHandle == newValueHandle)) break; /* no change */
	    newValueHandle = [ppl _store:value];
	    /* It is important to store new before decrementing the old ref count! */
	    SET_VALUE(newValueHandle, idx);
	    [ppl _decrRefCountInStoreFor:oldValueHandle];
	    break;
	}
	idx++;
	if (idx >= numBuckets) idx -= numBuckets;
    }
}

- (void)removeObjectForKey:(NSString *)key {
    _PPLStore	*store = [ppl _store];
    unsigned	hash = HASH_STR(key);
    unsigned	idx = hash % numBuckets;
    if (!ppl) zombieError(self, _cmd);
    if (!key) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil key", NSStringFromSelector(_cmd)];
    if (![key isKindOfClass:[NSString class]]) [NSException raise:NSInvalidArgumentException format:@"*** %@ key not a string:'%@'", NSStringFromSelector(_cmd), key]; // see bug #38650
    [ppl _setDirty];
    [self _hardenEnumerators];
    while (1) {
	unsigned	keyHandle = KEY(idx);
	if (!keyHandle) break;
	if ((HASH(idx) == hash) && [keyAtIndex(self, idx) isEqualToString:key]) {
	    /* was already in table!; we continue till the end of the chain, noting all the keys, removing them, and then we reinsert them */
	    unsigned	valueHandle = VALUE(idx);
	    unsigned	*oldHandles = NSZoneMalloc(NULL, numBuckets * 3 * WORD_SIZE);
	    unsigned	num = 0;
	    [self _zapAtIndex:idx];
	    [ppl _decrRefCountInStoreFor:keyHandle];
	    [ppl _decrRefCountInStoreFor:valueHandle];
	    count--;
	    [store writeUnsigned:count inBlock:handle atIndex:0];
	    idx++;
	    if (idx >= numBuckets) idx -= numBuckets;
	    while (1) {
		unsigned	bkeyHandle = KEY(idx);
		if (!bkeyHandle) break;
		[self _readHashKeyValueInto:oldHandles+(num*3) atIndex:idx];
		num++;
		[self _zapAtIndex:idx];
		idx++;
		if (idx >= numBuckets) idx -= numBuckets;
	    }
	    [self _reInsertPairs:num handles:oldHandles];
	    NSZoneFree(NSZoneFromPointer(oldHandles), oldHandles);
	    break;
	}
	idx++;
	if (idx >= numBuckets) idx -= numBuckets;
    }
}

- (NSEnumerator *)keyEnumerator {
    _PPLStore		*store = [ppl _store];
    PPLOrderedHeap	*allKeyHandles = PPLCreateOrderedHeap(NULL, count);
    _PPLMutableDictEnum	*enumerator;
    unsigned			idx = 0;
    if (!ppl) zombieError(self, _cmd);
    while (idx < numBuckets) {
	// important for VM: loop with increasing idxes
	unsigned	keyHandle = KEY(idx);
	if (keyHandle) PPLAddToOrderedHeap(allKeyHandles, (void *)keyHandle);
	idx++;
    }
    enumerator = [[_PPLMutableDictEnum allocWithZone:NULL] initWithDictionary:self allKeyHandles:allKeyHandles];
    numEnumerators++;
    enumerators = NSZoneRealloc(NSZoneFromPointer(enumerators), enumerators, numEnumerators * sizeof(id));
    enumerators[numEnumerators-1] = enumerator;
    return [enumerator autorelease];
}

- (void)_hardenEnumerators {
    while (numEnumerators) {
	_PPLMutableDictEnum	*enumerator = enumerators[--numEnumerators];
	unsigned		cc = PPLCountOrderedHeap(enumerator->allKeyHandles);
	if (cc) {
	    /* harden enumerator */
	    NSString *	key;
	    unsigned		idx = 0;
	    enumerator->numKeys = cc;
	    enumerator->allKeys = NSZoneMalloc(NULL, cc * sizeof(id));
	    while ( (key = retrieveFirstStringInHeap(ppl, enumerator->allKeyHandles)) ) {
		enumerator->allKeys[idx++] = [key retain];
	    }
	}
	/* We wipe out the dict field so that the enumerator is unable to call us back */
	[enumerator->dict release];
	enumerator->dict = nil; 
    }
    NSZoneFree(NSZoneFromPointer(enumerators), enumerators);
    enumerators = NULL;
}

- (void)_removeEnumerator:(_PPLMutableDictEnum *)enumerator {
    unsigned	idx = numEnumerators;
    while (idx--) {
	if (enumerators[idx] == enumerator) {
	    enumerators[idx] = enumerators[numEnumerators - 1];
	    numEnumerators--;
	    [enumerator->dict release]; // this may release self
	    enumerator->dict = nil;
	    return;
	}
    }
    _PPLBug(ppl, _cmd);
}

- (void)removeAllObjects {
    /* Speedup !*/
    _PPLStore	*store = [ppl _store];
    unsigned	idx = numBuckets;
    PPLOrderedHeap	*handles; // we sort handles for which to _decrRefCountInStoreFor: in order to improve locality
    if (!ppl) zombieError(self, _cmd);
    if (!count) return;
    [ppl _setDirty];
    [self _hardenEnumerators];
    handles = PPLCreateOrderedHeap(NULL, count);
    while (idx--) {
	unsigned	keyHandle = KEY(idx);
	if (keyHandle) {
	    unsigned	valueHandle = VALUE(idx);
	    [self _zapAtIndex:idx];
	    PPLAddToOrderedHeap(handles, (void *)keyHandle);
	    PPLAddToOrderedHeap(handles, (void *)valueHandle);
	}
    }
    count = 0;
    [store writeUnsigned:count inBlock:handle atIndex:0];
    [ppl _decrRefCountInStoreMultiple:handles];
    PPLFreeOrderedHeap(handles);
}

- (id)copyWithZone:(NSZone *)zone {
    if (!ppl) zombieError(self, _cmd);
    return [[NSDictionary allocWithZone:zone] initWithDictionary:self copyItems:YES];
} 



- (id)mutableCopyWithZone:(NSZone *)zone {
    if (!ppl) zombieError(self, _cmd);
    return [[NSMutableDictionary allocWithZone:zone] initWithDictionary:self];
} 



- (BOOL)isEqual:(id)other {
    if (!ppl) zombieError(self, _cmd);
    return (other == (id)self) || ([other isKindOfClass:[NSDictionary class]] && [self isEqualToDictionary:other]);
}

- (unsigned)hash {
    return count;
}

@end

/***************	Immutable object	***********/

@implementation _PPLPropertyList
- initWithRealPropertyList:plist {
    real = [plist retain];
    return self;
}

- (void)dealloc {
    [_PPLCache noteDeallocatingProxy:self];
    [real release];
    [super dealloc];
}

- (void)forwardInvocation:(NSInvocation *)invocation {
    if (!real) return;
    [invocation setTarget:real];
    [invocation invoke];
}

- (NSMethodSignature *)methodSignatureForSelector:(SEL)selector {
    return [real methodSignatureForSelector:selector];
}

- (NSString *)description {
    return [real description];
}

- (id)copyWithZone:(NSZone *)zone {
    return [self retain];
} 

- (id)copy {
    return [self copyWithZone:NULL];
}

- (id)mutableCopyWithZone:(NSZone *)zone {
    return [real mutableCopyWithZone:zone];
} 

- (id)mutableCopy {
    return [self mutableCopyWithZone:NULL];
}

- (BOOL)isEqual:(id)object {
    return [real isEqual:object]; 
}

- (unsigned)hash {
    return [real hash]; 
}

- (BOOL)isKindOfClass:(Class)cls {
    /* speed up, found worth it by sampling */
    return [real isKindOfClass:cls];
}

- (void)serializeIntoData:(NSMutableData *)mdata {
    [real serializeIntoData:mdata];
}

#if 1
// These are purely significant performance optimizations for mail back-end
- (NSArray *)allKeys {
    return [real allKeys];
}

- (id)objectForKey:(id)key {
    return [real objectForKey:key];
}

- (BOOL)respondsToSelector:(SEL)sel {
    return [real respondsToSelector:sel];
}

- (unsigned)count {
    return [real count];
}

- (id)objectAtIndex:(unsigned)idx {
    return [real objectAtIndex:idx];
}

- (NSEnumerator *)keyEnumerator { 
    return [real keyEnumerator];
}

- (unsigned)length {
    return [real length];
}

- (const void *)bytes {
    return [real bytes];
}

#endif

/* Common varargs for proxies, as forwarding doesn't work for varargs methods */
- (NSString *)stringByAppendingFormat:(NSString *)format, ... {
    NSString *result;
    NSString *string;
    va_list argList;
    va_start(argList, format);
    string = [[NSString allocWithZone:NULL] initWithFormat:format arguments:argList];
    va_end(argList);
    result = [real stringByAppendingString:string];
    [string release];
    return result;
}

- (void)appendFormat:(NSString *)format, ... {
    NSString *string;
    va_list argList;
    va_start(argList, format);
    string = [[NSString allocWithZone:NULL] initWithFormat:format arguments:argList];
    va_end(argList);
    [real replaceCharactersInRange:NSMakeRange([real length], 0) withString:string];
    [string release];
}

@end

/***************	Immutable PPL Data proxies		***********/

static id retrieve(_PPLReadOnlyStore *store, unsigned handle);

static id retrieveIndirect(_PPLReadOnlyStore *store, unsigned origin, unsigned idx) {
    unsigned	handle = [store unsignedForBlock:origin atIndex:idx];
    return retrieve(store, handle);
}

static id retrieve(_PPLReadOnlyStore *store, unsigned handle) {
    unsigned	type = [store readHeaderInBlock:handle].type;
    switch (type) {
	case STORED_NSSTRING:
	    return [store readNEXTSTEPStringInBlock:handle notifyCache:NO];
	case STORED_IOBJECT:
	    return [store readPropertyListInBlock:handle];
	case STORED_MARRAY:
	    return [[[_PPLDataArray allocWithZone:NULL] initWithPPLDataStore:store forHandle:handle] autorelease]; 
	case STORED_MDICT:
	    return [[[_PPLDataDictionary allocWithZone:NULL] initWithPPLDataStore:store forHandle:handle] autorelease]; 
	case STORED_FORWARD:
	    return retrieveIndirect(store, handle, 0);
	default:
	    [NSException raise:NSInternalInconsistencyException format:@"*** _PPLRetrieve() unknown ref code: %d", type];
	    return nil;
    }
}

@implementation _PPLDataArray

- initWithPPLDataStore:(_PPLReadOnlyStore *)ss forHandle:(unsigned)hh {
    store = [ss retain];
    handle = hh;
    count = [store unsignedForBlock:hh atIndex:0];
    return self;
}

- (unsigned)count {
    return count;
}

- objectAtIndex:(unsigned)idx {
    if (idx >= count) [NSException raise:NSRangeException format:@"*** -%@ %d is beyond count (%d)", NSStringFromSelector(_cmd), idx, count];
    return retrieveIndirect(store, handle, idx + 1);
}

- (void)dealloc {
    [store release];
    [super dealloc];
}

@end

@interface _PPLDataDictEnum:NSEnumerator {
    @public
    _PPLReadOnlyStore	*store;
    PPLOrderedHeap	*allKeyHandles; // all handles to enumerate;
	// we use a heap in order to seriously improve VM access
}

- initWithPPLDataStore:(_PPLReadOnlyStore *)store allKeyHandles:(PPLOrderedHeap *)allKeyHandles;
    /* transfer of ownership for allKeyHandles */

@end

@implementation _PPLDataDictEnum

- initWithPPLDataStore:(_PPLReadOnlyStore *)ss allKeyHandles:(PPLOrderedHeap *)akh {
    store = [ss retain];
    allKeyHandles = akh;
    return self;
}

- nextObject {
    while (PPLCountOrderedHeap(allKeyHandles)) {
	unsigned	handle = (unsigned)PPLRemoveMinimumOfOrderedHeap(allKeyHandles);
	unsigned	type = [store readHeaderInBlock:handle].type;
	switch (type) {
	    case STORED_NSSTRING:
		return [store readNEXTSTEPStringInBlock:handle notifyCache:NO];
	    case STORED_FORWARD:
		handle = [store unsignedForBlock:handle atIndex:0];
		PPLAddToOrderedHeap(allKeyHandles, (void *)handle);
		break;
	    default:
		[NSException raise:NSInternalInconsistencyException format:@"*** PPL retrieving an enumerator key: unknown ref code: %d", type];
		return nil;
	}
    }
    return nil; // make sure always returns nil if called repeatedly
}

- (void)dealloc {
    [store release];
    PPLFreeOrderedHeap(allKeyHandles);
    [super dealloc];
}

@end
    
@implementation _PPLDataDictionary

+ (NSDictionary *)retrieveRootDataStore:(_PPLReadOnlyStore *)ss forHandle:(unsigned)hh {
    return retrieve(ss, hh);
}

- initWithPPLDataStore:(_PPLReadOnlyStore *)ss forHandle:(unsigned)hh {
    BlockHeader	header = [ss readHeaderInBlock:hh];
    store = [ss retain];
    handle = hh;
    numBuckets = bucketsForCapacity(nil, header.capacity);
    count = [store unsignedForBlock:hh atIndex:0];
    if (!numBuckets) [NSException raise:NSInternalInconsistencyException format:@"*** %@ no buckets", NSStringFromSelector(_cmd)];
    if (count > numBuckets) [NSException raise:NSInternalInconsistencyException format:@"*** %@ count too large", NSStringFromSelector(_cmd)];
    return self;
}

- (void)dealloc {
    [store release];
    [super dealloc];
}

- (unsigned)count {
    return count;
}

- objectForKey:(NSString *)key {
    unsigned	hash = HASH_STR(key);
    unsigned	idx = hash % numBuckets;
    id		value = nil;
    if (!key) [NSException raise:NSInvalidArgumentException format:@"*** %@ nil key", NSStringFromSelector(_cmd)];
    if (![key isKindOfClass:[NSString class]]) [NSException raise:NSInvalidArgumentException format:@"*** %@ key not a string:'%@'", NSStringFromSelector(_cmd), key]; // see bug #38650
    while (1) {
	unsigned	keyHandle = KEY(idx);
	if (!keyHandle) break;
	if ((HASH(idx) == hash) && keyHandle && [retrieveIndirect(store, handle, idx * 3 + 2) isEqualToString:key]) {
	    /* we are careful to not create the key if it is the wrong hash */
	    unsigned	valueHandle = VALUE(idx);
	    if (!valueHandle) [NSException raise:NSInternalInconsistencyException format:@"*** Damaged dictionary: !valueHandle"];
	    value = retrieveIndirect(store, handle, idx * 3 + 3);
	    break;
	}
	idx++;
	if (idx >= numBuckets) idx -= numBuckets;
    }
    return value;
}

- (NSEnumerator *)keyEnumerator {
    PPLOrderedHeap	*allKeyHandles = PPLCreateOrderedHeap(NULL, count);
    unsigned		idx = 0;
    while (idx < numBuckets) {
	// important for VM: loop with increasing idxes
	unsigned	keyHandle = KEY(idx);
	if (keyHandle) PPLAddToOrderedHeap(allKeyHandles, (void *)keyHandle);
	idx++;
    }
    return [[[_PPLDataDictEnum allocWithZone:NULL] initWithPPLDataStore:store allKeyHandles:allKeyHandles] autorelease];
}

@end

@implementation _PPLMutables

- init {
    mobjectToHandle = NSCreateMapTableWithZone(NSNonOwnedPointerMapKeyCallBacks, NSIntMapValueCallBacks, 0, NULL);
    handleToMObject = NSCreateMapTableWithZone(NSIntMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0, NULL);
    handlesToDestroy = NSCreateHashTableWithZone(NSIntHashCallBacks, 0, NULL);
    return self;
}

- (void)dealloc {
    if (NSCountMapTable(mobjectToHandle)) PPLLog(@"*** -[PPL dealloc] mobjectToHandle not empty");
    NSFreeMapTable(mobjectToHandle);
    if (NSCountMapTable(handleToMObject)) {
	PPLLog(@"*** -[PPL dealloc] handleToMObject not empty; indicates objects retained beyond destruction");
    }
    NSFreeMapTable(handleToMObject);
    NSFreeHashTable(handlesToDestroy);
    [super dealloc];
}

- (BOOL)isEmpty {
    if (NSCountMapTable(mobjectToHandle)) return NO;
    if (NSCountMapTable(handleToMObject)) return NO;
    if (NSCountHashTable(handlesToDestroy)) return NO;
    return YES;
}

/***************	Handles to objects	***************/

- (void)noteHandle:(unsigned)mhandle forMutableObject:mutableObject {
    [mutableObject retain];
    NSMapInsert(handleToMObject, (void *)mhandle, mutableObject);
    NSMapInsert(mobjectToHandle, mutableObject, (void *)mhandle);
}

- (void)noteHandle:(unsigned)mhandle forMutableObjectNonRetained:mutableObject {
    NSMapInsert(handleToMObject, (void *)mhandle, mutableObject);
    NSMapInsert(mobjectToHandle, mutableObject, (void *)mhandle);
}

- mutableObjectForHandle:(unsigned)mhandle {
    return (id)NSMapGet(handleToMObject, (void *)mhandle);
}

- (unsigned)handleForMutableObject:value {
    return (unsigned)NSMapGet(mobjectToHandle, value);
}

- (unsigned)getHandlesToMutableObjectsInto:(unsigned **)refHandles {
    unsigned	count = NSCountMapTable(handleToMObject);
    unsigned	idx = 0;
    id		mutableObject;
    unsigned	mhandle;
    NSMapEnumerator	state = NSEnumerateMapTable(handleToMObject);
    if (!count) return 0;
    *refHandles = NSZoneMalloc(NULL, count * sizeof(unsigned));
    while (NSNextMapEnumeratorPair(&state, (void **)&mhandle, (void **)&mutableObject)) {
	(*refHandles)[idx] = mhandle;
	idx++;
    }
    return idx;
}

- (BOOL)forgetMutableObjectHandle:(unsigned)mhandle {
    id 	mutableObject = (id)NSMapGet(handleToMObject, (void *)mhandle);
    if (mutableObject) {
	NSMapRemove(handleToMObject, (void *)mhandle);
	NSMapRemove(mobjectToHandle, mutableObject);
	[mutableObject release];
    }
    return mutableObject != nil;
}

- (unsigned)mutableObjectsCount {
    return NSCountMapTable(handleToMObject);
}

- (BOOL)_hasNonProxyMutable {
    id		mutableObject;
    unsigned	mhandle;
    Class	marrayClass = [NSMutableArray class];
    Class	mdictClass = [NSMutableDictionary class];
    NSMapEnumerator	state = NSEnumerateMapTable(handleToMObject);
    while (NSNextMapEnumeratorPair(&state, (void **)&mhandle, (void **)&mutableObject)) {
	if ([mutableObject isKindOfClass:marrayClass] || [mutableObject isKindOfClass:mdictClass]) return YES;
    }
    return NO;
}

/***************	Objects to destroy	***************/

- (void)noteMutableHandleToDestroy:(unsigned)mhandle {
    NSHashInsert(handlesToDestroy, (void *)mhandle);
}

- (BOOL)forgetMutableHandleToDestroy:(unsigned)mhandle {
    if (! NSHashGet(handlesToDestroy, (void *)mhandle)) return NO;
    NSHashRemove(handlesToDestroy, (void *)mhandle);
    return YES;
}

- (unsigned)getHandlesToDestroyInto:(unsigned **)refHandles {
    unsigned	count = NSCountHashTable(handlesToDestroy);
    if (count) {
	unsigned	idx = 0;
	unsigned	handle;
	NSHashEnumerator	state = NSEnumerateHashTable(handlesToDestroy);
	*refHandles = NSZoneMalloc(NULL, count * sizeof(unsigned));
	while ((handle = (unsigned)NSNextHashEnumeratorItem(&state))) {
	    (*refHandles)[idx++] = handle;
	}
    }
    return count;
}

- (unsigned)handleToDestroyCount {
    return NSCountHashTable(handlesToDestroy);
}

static void *oneOfMapTable(NSMapTable *table, void **key) {
    NSMapEnumerator	state = NSEnumerateMapTable(table);
    void	*value;
    if (NSNextMapEnumeratorPair(&state, key, &value)) {
	return value;
    }
    return NULL;
}

- _oneMutable {
    unsigned	handle;
    return oneOfMapTable(handleToMObject, (void **)&handle);
}

- (NSString *)description {
    id		mutableObject;
    unsigned	mhandle;
    NSMapEnumerator	state1 = NSEnumerateMapTable(handleToMObject);
    NSHashEnumerator	state2 = NSEnumerateHashTable(handlesToDestroy);
    NSMutableString	*str = [NSMutableString stringWithCString:"PPLMutables: mutable objects = (" length:strlen("PPLMutables: mutable objects = (")];
    BOOL	first = YES;
    while (NSNextMapEnumeratorPair(&state1, (void **)&mhandle, (void **)&mutableObject)) {
	if (first) first = NO; else [str replaceCharactersInRange:NSMakeRange([str length], 0) withString:@", "];
	[str appendFormat:@"^%x 0x%x", mhandle, mutableObject];
    }
    first = YES; [str replaceCharactersInRange:NSMakeRange([str length], 0) withString:@") to destroy = ("];
    while ((mhandle = (unsigned)NSNextHashEnumeratorItem(&state2))) {
	if (first) first = NO; else [str replaceCharactersInRange:NSMakeRange([str length], 0) withString:@", "];
	[str appendFormat:@"^%x", mhandle];
    }
    [str replaceCharactersInRange:NSMakeRange([str length], 0) withString:@")\n"];
    return str;
}

- (NSString *)_debug {
    return [NSString stringWithFormat:@"%d mutable objects in mapping; %d to destroy", NSCountMapTable(handleToMObject), NSCountHashTable(handlesToDestroy)];
}

@end

/***************	Definitions	***************/


#define CURRENT_VERSION 	3
#define MAGIC_NUMBER_V3		0xbaba0003
#define CURRENT_MAGIC		MAGIC_NUMBER_V3

#define OFF_CAPACITY		WORD_SIZE	// denotes the useful length in bytes (i.e. without the 3-word header)

#define FILE_HEADER_LEN		(4 * WORD_SIZE)

#define ROOT_OFFSET		(3 * WORD_SIZE)

#define APPEND_QUANTUM	(1024)

/***************	Utilities	***************/

static BOOL _debugDataStore = NO;
static BOOL _enableRemap = YES;
static BOOL _enableIncrementalCompaction = YES;
static BOOL _enableIncrementalCompactionLog = NO;
static BOOL _checkRemapSame = NO; // YES makes things slow ...
static unsigned mapThreshold = (2*8*1024);

static unsigned roundUpChunk(unsigned length) {
    return ((length + MIN_CHUNK - 1) / MIN_CHUNK) * MIN_CHUNK;
}

static unsigned packFirst(BlockHeader header) {
    if (header.refCount >> 24) [NSException raise:NSInternalInconsistencyException format:@"*** packFirst() refcount = %d", header.refCount];
    if (header.type >> 8) [NSException raise:NSInternalInconsistencyException format:@"*** packFirst() type = %d", header.type];
    return (header.refCount << 8) + header.type;
}

static BlockHeader readHeader(NSData *data, unsigned location) {
    BlockHeader	header;
    unsigned		nums[2];
    [data deserializeInts:nums count:2 atIndex:location];
    header.refCount = nums[0] >> 8;
    header.type = nums[0] & 0xff;
    header.capacity = nums[1];
    return header;
}

static NSString *dataPath(NSString *path) {
    return [path stringByAppendingPathComponent:@"store"];
}

static NSString *logPath(NSString *path) {
    return [path stringByAppendingPathComponent:@"log"];
}

static unsigned validateBlocks(NSData * data, id <PPLTypeProvider>typeProvider) {
    unsigned	location;
    unsigned	magic;
    unsigned	handle;
    int		refCountSum = -1;
    [data getBytes:&magic length:4];
    if ((magic != MAGIC_NUMBER_V3) && (magic != NSSwapInt(MAGIC_NUMBER_V3))) {
	return 0;
    }
    handle = [data deserializeIntAtIndex:ROOT_OFFSET]; /* first handle */
    if (handle > [data length] - MIN_CHUNK) {
	PPLLog(@"*** Store header badly damaged; Store is probably unusable");
	/* We continue because there not much else to do ... */
    }
    location = FILE_HEADER_LEN;
    while (1) {
	BlockHeader	header;
	unsigned	extra;
	if (location == [data length]) {
	    /* everything is fine! */
	    if (refCountSum) PPLLog(@"*** Refcounts do not add up: %d.  There may be storage leak or even store damage", refCountSum);
	    return location;
	}
	if (location > [data length] - MIN_CHUNK) {
	    /* this block was bogus! */
	    return handle;
	}
	header = readHeader(data, location);
	refCountSum += header.refCount;
	extra = [data deserializeIntAtIndex:location + BLOCK_HEADER_LEN];
	handle = location;
	if (![typeProvider checkValidHeader:header]) {
	    PPLLog(@"*** Encountered invalid header for type 0x%x for handle ^%x; Store file is damaged", header.type, handle);
	} else {
	    unsigned	idx = header.capacity / WORD_SIZE;
	    while (idx--) {
		if ([typeProvider isSubHandleForHeader:header extra:extra atIndex:idx]) {
		    unsigned	value;
		    value = [data deserializeIntAtIndex:location + BLOCK_HEADER_LEN + idx * WORD_SIZE];
		    if (value && !canBeHandle(value)) {
			PPLLog(@"*** Encountered 0x%x for handle ^%x at idx %d; Store file is damaged", value, handle, idx);
		    } else if (value) {
			refCountSum --;
		    }
		}
	    }
	}
	location += roundUpChunk(header.capacity + BLOCK_HEADER_LEN);
    }
}

static void reflectChangesInData(_PPLRangeArray *ranges, NSData *current, NSFileHandle *datafd) {
    /* Note: we must seek to the end of the file after calling this or things will be inconsistent;
    May raise when writing datafd */
    unsigned	length = [current length];
    NSRange	*rr = ranges->ranges;
    unsigned	count = ranges->count;
    while (count--) {
	NSRange	range = *rr;
	if ((range.location & 3) || (range.length & 3)) [NSException raise:NSInternalInconsistencyException format:@"*** reflectChangesInData() Alignment error: %d %d", range.location, range.length];
	if (range.location + range.length > length) [NSException raise:NSInternalInconsistencyException format:@"*** reflectChangesInData() Seeking beyond the end - can't happen: %d", length];
	[datafd seekToFileOffset:(unsigned long long)range.location];
	if (_debugDataStore) PPLLog(@"Writing %d words in DATA file at 0x%x", range.length / WORD_SIZE, range.location);
	[datafd writeData:[current subdataWithRange:range]];
	rr++;
    }
}

/***************	Sorted Range utilities	***************/

static BOOL isInRanges(unsigned candidate, NSRange *ranges, unsigned numRanges, unsigned *hintIndex) {
    /* In input, *hintIndex indicates where to start from (use numRanges for starting in the middle);
    On output, *hintIndex is set to the idx when found */
    if (!numRanges) return NO;
    if (*hintIndex >= numRanges) *hintIndex = numRanges / 2;
    if (NSLocationInRange(candidate, ranges[*hintIndex])) return YES;
    if (candidate < ranges[*hintIndex].location) {
	return isInRanges(candidate, ranges, *hintIndex, hintIndex);
    } else {
	unsigned	numToSkip = *hintIndex + 1;
	BOOL		res = isInRanges(candidate, ranges+numToSkip, numRanges-numToSkip, hintIndex);
	*hintIndex += numToSkip;
	return res;
    }
}

static unsigned findNumRangesTotallyBefore(unsigned candidate, NSRange *ranges, unsigned numRanges, unsigned *hintIndex) {
    /* given a candidate location, a bunch of ranges, and a hint where to start,
    returns the number of ranges totally before location */
    if (!numRanges) return 0;
    if (*hintIndex >= numRanges) *hintIndex = numRanges / 2;
    if (NSLocationInRange(candidate, ranges[*hintIndex])) return *hintIndex;
    if (candidate < ranges[*hintIndex].location) {
	return findNumRangesTotallyBefore(candidate, ranges, *hintIndex, hintIndex);
    } else {
	unsigned	numToSkip = *hintIndex + 1;
	unsigned	res = findNumRangesTotallyBefore(candidate, ranges+numToSkip, numRanges-numToSkip, hintIndex);
	*hintIndex += numToSkip;
	return res + numToSkip;
    }
}

static BOOL areRangesMergeable(NSRange r1, NSRange r2) {
    if (NSIntersectionRange(r1, r2).length) return YES;
    return (r1.location == NSMaxRange(r2)) || (r2.location == NSMaxRange(r1));
}

static void copyRanges(NSRange *source, NSRange *dest, unsigned num) {
    memmove(dest, source, num * sizeof(NSRange));
}

static void addRangeToRanges(NSRange candidate, NSRange **ranges, unsigned *numRanges, unsigned *hintIndex) {
    /* *hintIndex indicates where to start from (use numRanges for starting in the middle) */
    unsigned	idx = findNumRangesTotallyBefore(candidate.location, *ranges, *numRanges, hintIndex);
    NSRange	*rr = (*ranges) + idx;
    /* idx designates the first 'to be merge' range */
    if ((idx >= *numRanges) || !areRangesMergeable(candidate, *rr)) {
	*ranges = NSZoneRealloc(NSZoneFromPointer(*ranges), *ranges, ((*numRanges)+1)*sizeof(NSRange));
	rr = (*ranges) + idx; /* since *ranges may have changed */
	copyRanges(rr, rr+1, (*numRanges) - idx);
	*rr = candidate;
	(*numRanges)++;
	return;
    }
    *rr = NSUnionRange(*rr, candidate);
    while ((idx + 1 < (*numRanges)) && areRangesMergeable(*rr, *(rr+1))) {
	*rr = NSUnionRange(*rr, *(rr+1));
	copyRanges(rr+2, rr+1, (*numRanges) - idx - 2);
	(*numRanges)--;
    }
}

/***************	Common root store class	***************/

@implementation _PPLStoreRoot

/***************	Block access	***************/

static BlockHeader readBlockHeaderAndCompact(_PPLStoreRoot *self, unsigned handle) {
    BlockHeader	header;
    if (!self->quadLocation || (self->quadLocation != handle)) {
	[self->current deserializeInts:self->quadUnsigned count:4 atIndex:handle];
	self->quadLocation = handle;
    }
    header.refCount = self->quadUnsigned[0] >> 8;
    header.type = self->quadUnsigned[0] & 0xff;
    header.capacity = self->quadUnsigned[1];
    if (_enableIncrementalCompaction && header.type) {
	[self autoCompactForHeader:header inBlock:handle];
    }
    return header;
}

static unsigned unsignedAt(_PPLStoreRoot *self, unsigned location) {
    unsigned	quadded = location & ~ (MIN_CHUNK - 1);
    if (!self->quadLocation || (self->quadLocation != quadded)) {
	[self->current deserializeInts:self->quadUnsigned count:4 atIndex:quadded];
	self->quadLocation = quadded;
    }
    return self->quadUnsigned[(location - quadded) >> 2];
}

- (unsigned)rootIndex {
    return (ROOT_OFFSET - BLOCK_HEADER_LEN) / WORD_SIZE;
}

- (unsigned)totalRoundedLengthOfBlock:(unsigned)handle {
    return roundUpChunk(readBlockHeaderAndCompact(self, handle).capacity + BLOCK_HEADER_LEN);
}

- (BlockHeader)readHeaderInBlock:(unsigned)handle {
    return readBlockHeaderAndCompact(self, handle);
}

- (unsigned)unsignedForBlock:(unsigned)handle atIndex:(unsigned)idx {
    unsigned	location = handle + BLOCK_HEADER_LEN + idx * WORD_SIZE;
    return unsignedAt(self, location);
} 

- (void)readDataInBlock:(unsigned)handle intoData:(NSMutableData *)mdata {
    unsigned	length = unsignedAt(self, handle + OFF_CAPACITY);
    unsigned	location = handle + BLOCK_HEADER_LEN;
    [mdata appendBytes:[current bytes] + location length:length];
}

- readPropertyListInBlock:(unsigned)handle {
    unsigned	cursor = handle + BLOCK_HEADER_LEN;
    unsigned	length = unsignedAt(self, handle + OFF_CAPACITY);
    unsigned	max = cursor + length;
    id		plist = [NSDeserializer deserializePropertyListLazilyFromData:current atCursor:&cursor length:length mutableContainers:NO];
    if (cursor > max) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
    return plist;
}

- (NSString *)readNEXTSTEPStringInBlock:(unsigned)handle notifyCache:(BOOL)cache {
    NSRange	range = {handle + BLOCK_HEADER_LEN, unsignedAt(self, handle + OFF_CAPACITY)};
    char	*chars = NSZoneMalloc(NULL, range.length);
    NSString	*str;
    [current getBytes:chars range:range];
    str = [[NSString alloc] initWithData:[NSData dataWithBytesNoCopy:chars length:range.length] encoding:NSNEXTSTEPStringEncoding];
    if (cache)
        str = [[[_PPLString alloc] initWithString:str] autorelease];
    return str;
}

- (void)initString:(_PPLString *)string inBlock:(unsigned)handle {
    NSRange	range = {handle + BLOCK_HEADER_LEN, unsignedAt(self, handle + OFF_CAPACITY)};
    char	*chars = NSZoneMalloc(NULL, range.length);
    NSString	*str;
    [current getBytes:chars range:range];
    str = [[NSString alloc] initWithData:[NSData dataWithBytesNoCopy:chars length:range.length] encoding:NSNEXTSTEPStringEncoding];
    [string initWithString:str];
}

- (void)autoCompactForHeader:(BlockHeader)header inBlock:(unsigned)handle {
}

@end

/***************	Logging store	***************/

@implementation _PPLStore

static int versionForData(const char *pplData) {
    unsigned	magic;
    int		vers = -4;
    magic = *((unsigned *)pplData);
    if (magic == MAGIC_NUMBER_V3) {
	vers = CURRENT_VERSION;
    } else if (magic == NSSwapInt(MAGIC_NUMBER_V3)) {
	vers = CURRENT_VERSION;
    }
    return vers;
}

static void _PPLFileAccessModes(NSString *path, BOOL *exists, BOOL *readable, BOOL *writeable, BOOL *executable, BOOL *isDir, unsigned long long *size) {
    char cpath[1040];
    struct stat statBuf;
    int result;

    if (exists) *exists = NO;
    if (readable) *readable = NO;
    if (writeable) *writeable = NO;
    if (executable) *executable = NO;
    if (isDir) *isDir = NO;
    if (size) *size = 0;
    if (![path getFileSystemRepresentation:cpath maxLength:1040])
        return;
    result = stat(cpath, &statBuf);
    if (result == 0) {
        if (exists) *exists = YES;
        if (readable) *readable = (access(cpath, 4) == 0);
        if (writeable) *writeable = (access(cpath, 2) == 0);
        if (executable) *executable = (access(cpath, 1) == 0);
        if (isDir) *isDir = ((statBuf.st_mode & S_IFMT) == S_IFDIR);
        if (size) *size = (unsigned long long)statBuf.st_size;
    }
}

static BOOL _PPLFileAccessibleForMode(NSString *path, int mode) {
    char cpath[1040];
    if (![path getFileSystemRepresentation:cpath maxLength:1040]) return NO;
    return !access(cpath, mode);
}

+ (int)versionForStoreAtPath:(NSString *)apath writable:(BOOL *)writable {
    NSString		*dpath;
    char		header[FILE_HEADER_LEN];
    int			fd;
    int			vers = -4;
    BOOL exists, reading, writing, isDir;
    unsigned long long fileSize;

    _PPLFileAccessModes(apath, &exists, &reading, &writing, NULL, &isDir, NULL);
    if (!exists) return -1;
    if (!reading) return -3;
    if (!isDir) return -2;
    *writable = writing;
    dpath = dataPath(apath);
    _PPLFileAccessModes(dpath, &exists, &reading, &writing, NULL, NULL, &fileSize);
    if (!exists) return -1;
    if (!reading) return -3;
    *writable = *writable && writing;
    if (fileSize < FILE_HEADER_LEN + MIN_CHUNK) return -4;
#if defined(WIN32)
    fd = open([dpath fileSystemRepresentation], O_RDONLY | _O_BINARY| _O_NOINHERIT, 0);
#else
    fd = open([dpath fileSystemRepresentation], O_RDONLY, 0);
#endif
    if (fd < 0)
    	return -3;
    read(fd, header, FILE_HEADER_LEN);
    vers = versionForData(header);
    close(fd);
    return vers;
}

+ (int)versionForStoreAtPath:(NSString *)apath withPPLData:(NSData *)pplData {
if (_PPLFileAccessibleForMode(apath,  0)) return -5;
if (!_PPLFileAccessibleForMode([apath stringByDeletingLastPathComponent],  2)) return -6;
    if ([pplData length] < FILE_HEADER_LEN + MIN_CHUNK) return -4;
    return versionForData([pplData bytes]);
}

static unsigned logPage = 0;
- commonInit {
    if (readOnly) {
	/* Now that current has been scavenged (potentially) we freeze it by making it immutable; that way, -subdataWithRange: is much cheaper */
	NSMutableData *	oldCurrent = current;
	current = [current copyWithZone:NULL];
	[oldCurrent release];
    } else {
	freeInfo = [[_PPLFreeInfo allocWithZone:NULL] init];
    }
    numCleanRanges = 1;
    if (!logPage) logPage = NSLogPageSize();
    cleanRanges = NSZoneMalloc(NULL, sizeof(NSRange));
    cleanRanges[0] = NSMakeRange((([current length] >> logPage) << logPage), 0xffffffff); /* let's not compact the end of the file */ 
    cleanRanges[0].length -= cleanRanges[0].location; /* we do that so that we never overflow during range computations */
    return self;
}

- initWithNewStorePath:(NSString *)apath typeProvider:(id <PPLTypeProvider>)tp {
    unsigned	magic = CURRENT_MAGIC;
    NSMutableData	*pplData = [NSMutableData dataWithCapacity:FILE_HEADER_LEN + MIN_CHUNK];
    [pplData appendBytes:&magic length:WORD_SIZE]; /* we write in native format! */
    [pplData serializeInt:0]; /* dummy 1 */
    [pplData serializeInt:0]; /* dummy 2 */
    [pplData serializeInt:0]; /* future location of the root handle */
    return [self initWithNewStorePath:apath fromPPLData:pplData readOnly:NO typeProvider:tp];
}

- initWithNewStorePath:(NSString *)apath fromPPLData:(NSData *)pplData readOnly:(BOOL)ro typeProvider:(id <PPLTypeProvider>)tp {
    NSString 	*dpath = dataPath(apath);
    int fd;
    path = [apath copyWithZone:NULL];
    typeProvider = tp;
    if (!_PPLFileAccessibleForMode(path, 4)) {
        char cpath[1040];
        if (![path getFileSystemRepresentation:cpath maxLength:1040]) return NO;
        if (!
#if defined(WIN32)
            CreateDirectoryA(cpath, (LPSECURITY_ATTRIBUTES)NULL)
#else
            mkdir(cpath, 0777) == 0
#endif
            ) {
	    PPLLog(@"*** Cannot create %@", path);
	    goto nope;
	}
    }
    if (_PPLFileAccessibleForMode(dpath, 4)) {
	PPLLog(@"*** File %@ already exists", dpath);
	goto nope;
    }
#if defined(WIN32)
    fd = open([dpath fileSystemRepresentation], O_RDWR | O_CREAT | O_TRUNC | _O_BINARY| _O_NOINHERIT, _S_IREAD | _S_IWRITE);
#else
    fd = open([dpath fileSystemRepresentation], O_RDWR | O_CREAT | O_TRUNC, 0666);
#endif
    if (0 <= fd)
	datafd = [[NSFileHandle allocWithZone:NULL] initWithFileDescriptor:fd closeOnDealloc:YES];
    if (!datafd) {
	PPLLog(@"*** Can't create data file %@", path);
	goto nope;
    }
    readOnly = ro; version = CURRENT_VERSION;
    if (ro) current = [pplData copyWithZone:NULL];
    else current = [pplData mutableCopyWithZone:NULL];
    NS_DURING
	[datafd writeData:current];
	[datafd synchronizeFile];
	dataNeedsSync = NO;
	lastLengthSyncedData = [current length];
    NS_HANDLER
	PPLLog(@"*** Can't write data file %@", path);
	goto nope;
    NS_ENDHANDLER
    NS_DURING
	logInfo = [[_PPLLogInfo allocWithZone:NULL] initAndRefreshLogForPath:logPath(path)];
	[logInfo appendLogMarkerAndSync:lastLengthSyncedData];
    NS_HANDLER
	PPLLog(@"*** Can't create and write log file %@", path);
	goto nope;
    NS_ENDHANDLER
    if (!logInfo) goto nope;
    return [self commonInit];
  nope:
    [self dealloc]; 
    return nil;
}

- initWithExistingStoreAtPath:(NSString *)apath readOnly:(BOOL)ro typeProvider:(id <PPLTypeProvider>)tp {
    NSString *	dpath = dataPath(apath);
    unsigned		magic;
    id pool = [NSAutoreleasePool new];
    NSRange		range = {0, 0};
    path = [apath copyWithZone:NULL];
    typeProvider = tp;
    readOnly = ro;
    if (!_PPLFileAccessibleForMode(dpath, 4)) {
	PPLLog(@"*** Can't read file at %@", dpath);
	goto nope;
    }
    if (readOnly)
	datafd = [[NSFileHandle fileHandleForReadingAtPath:dpath] retain];
    else
	datafd = [[NSFileHandle fileHandleForUpdatingAtPath:dpath] retain];
    if (datafd)
	lastLengthSyncedData = (unsigned long)[datafd seekToEndOfFile];
    else {
	PPLLog(@"*** Can't read data file %@", path);
	goto nope;
    }
    range.length = (unsigned int)lastLengthSyncedData;
    [datafd seekToFileOffset:range.location];
    current = [[NSMutableData allocWithZone:NULL] initWithData:[datafd readDataOfLength:range.length]];
    [current getBytes:&magic length:WORD_SIZE];
    if (magic == MAGIC_NUMBER_V3) {
	version = CURRENT_VERSION;
    } else if (magic == NSSwapInt(MAGIC_NUMBER_V3)) {
	version = CURRENT_VERSION;
    } else {
	PPLLog(@"*** Invalid store file at %@: 0x%x", path, magic);
	goto nope;
    }
    switch ([_PPLLogInfo recoverLogForPath:logPath(path) applyToData:current]) {
	case LogNormal:  /* everything's cool */
	    if (!readOnly) {
		logInfo = [[_PPLLogInfo allocWithZone:NULL] initWithExistingLogForPath:logPath(path)];
	    }
	    break;
	case LogTruncated:
	    lastLengthSyncedData = [current length];
	    if (!readOnly) {
		PPLLog(@"*** Truncating data file to 0x%x", (unsigned)lastLengthSyncedData);
		NS_DURING
		    [datafd truncateFileAtOffset:(unsigned long long)lastLengthSyncedData];
		    [datafd synchronizeFile];
		    [datafd seekToEndOfFile];
		    logInfo = [[_PPLLogInfo allocWithZone:NULL] initWithExistingLogForPath:logPath(path)];
		NS_HANDLER
		    goto nope;
		NS_ENDHANDLER
	    }
	    break;
	case LogScavenged:
	    lastLengthSyncedData = [current length];
	    if (!readOnly) {
		NS_DURING
		    int fd;
		    [datafd release];
		    datafd = nil;
		    _PPLMoveToBackup(dpath);
#if defined(WIN32)
			fd = open([dpath fileSystemRepresentation], O_RDWR | O_CREAT | O_TRUNC | _O_BINARY| _O_NOINHERIT, _S_IREAD | _S_IWRITE);
#else
			fd = open([dpath fileSystemRepresentation], O_RDWR | O_CREAT | O_TRUNC, 0666);
#endif
		    if (0 <= fd)
			datafd = [[NSFileHandle allocWithZone:NULL] initWithFileDescriptor:fd closeOnDealloc:YES];
                    if (!datafd) {
                        [NSException raise:@"junk" format:@""];
                    }
		    [datafd writeData:current];
		    [datafd synchronizeFile];
		    logInfo = [[_PPLLogInfo allocWithZone:NULL] initAndRefreshLogForPath:logPath(path)];
		    [logInfo appendLogMarkerAndSync:lastLengthSyncedData];
		NS_HANDLER
		    PPLLog(@"*** Can't create scavenged data file %@", path);
		    goto nope;
		NS_ENDHANDLER
	    }
	    break;
	case LogUnsuccessfullyApplied:
	case LogUnusable: {
	    /* log is unusable; we re-validate the entire current */
	    unsigned	truncLoc = validateBlocks(current, typeProvider);
	    if (truncLoc == lastLengthSyncedData) {
		PPLLog(@"Store file passes basic consistency check");
	    } else {
		PPLLog(@"*** Inconsistent blocks found between 0x%x and 0x%x; truncating", truncLoc, (unsigned)lastLengthSyncedData);
		if (!readOnly) {
		    NS_DURING
			[datafd truncateFileAtOffset:(unsigned long long)truncLoc];
			[datafd synchronizeFile];
			[datafd seekToEndOfFile];
		    NS_HANDLER
			PPLLog(@"*** Can't truncate scavenged data file %@", path);
			goto nope;
		    NS_ENDHANDLER
		}
		[current setLength:truncLoc];
		lastLengthSyncedData = truncLoc;
	    }
	    if (!readOnly) {
		NS_DURING
		    logInfo = [[_PPLLogInfo allocWithZone:NULL] initAndRefreshLogForPath:logPath(path)];
		    [logInfo appendLogMarkerAndSync:lastLengthSyncedData];
		NS_HANDLER
		    PPLLog(@"*** Can't create proper log file %@", path);
		    goto nope;
		NS_ENDHANDLER
	    }
	    break;
	}
    }
    if (!logInfo) readOnly = YES;
    [pool release];
    return [self commonInit];
  nope:
    [self dealloc];
    [pool release];
    return nil;
}

- (void)dealloc {
    [datafd release];
    [path release];
    [current release];
    [freeInfo release];
    [logInfo release];
    NSZoneFree(NSZoneFromPointer(cleanRanges), cleanRanges);
    [super dealloc];
}

- (void)noteInvalid {
    PPLLog(@"*** Fatal error occured; disabling store");
    [datafd release];
    datafd = nil;
    [freeInfo release];
    freeInfo = nil;
    [logInfo release];
    logInfo = nil;
}

- (NSString *)path {
    return path;
}

/***************	Block creation / modification	***************/

static void willModifyStore(_PPLStore *self) {
    if (self->readOnly) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLStore willModifyStore(): Can't write read only store"];
    if (!self->datafd) [NSException raise:NSInternalInconsistencyException format:@"*** _PPLStore willModifyStore(): Can't write store that has previously encountered fatal error"];
    self->dataNeedsSync = YES;
}

static void writeUnsigned(_PPLStore *self, unsigned newValue, unsigned location) {
    /* May raise when writing on log fd */
    unsigned	oldValue = unsignedAt(self, location);
    if (oldValue == newValue) return;
    willModifyStore(self);
    /* Whether or not location is past lastLengthSyncedData, we write into the log so that we will on -reflectCurrentOnDisk modify the data file */
    if (_debugDataStore) PPLLog(@"Writing in LOG for location 0x%x : original=0x%x", location, oldValue);
    [self->logInfo appendLogChangeAt:location original:oldValue];
    self->logNeedsTruncate = YES;
    [self->current serializeInt:(int)newValue atIndex:location];
    self->quadLocation = 0;
}

static void write4Zeros(_PPLStore *self, unsigned location) {
    /* May raise when writing on log fd */
    unsigned	old[4];
    [self->current deserializeInts:old count:4 atIndex:location];
    if (!old[0] && !old[1] && !old[2] && !old[3]) return;
    willModifyStore(self);
    /* Whether or not location is past lastLengthSyncedData, we write into the log so that we will on -reflectCurrentOnDisk modify the data file */
    if (_debugDataStore) PPLLog(@"Writing in LOG 4 zeros for location 0x%x", location);
    [self->logInfo appendLogChangesAt:location original4:old];
    self->logNeedsTruncate = YES;
    old[0] = 0; old[1] = 0; old[2] = 0; old[3] = 0; /* old -> new */
    [self->current serializeInts:old count:4 atIndex:location];
}

static void finishAppendingToDatafd(_PPLStore *self) {
    /* May raise when writing */
    NSRange	range = {[self->current length] - self->lengthToBeAppended, self->lengthToBeAppended};
    [self->datafd writeData:[self->current subdataWithRange:range]];
    self->lengthToBeAppended = 0;
}

- (unsigned)createBlockForHeader:(BlockHeader)header andData:(NSData *)data reuseFreeBlock:(BOOL)reuse cold:(BOOL)cold trueCapacity:(unsigned *)trueCapacity {
    unsigned	length = [data length];
    unsigned	handle = 0;
    unsigned	roundedLength = roundUpChunk(length + BLOCK_HEADER_LEN);
    unsigned	padLength = roundedLength - length - BLOCK_HEADER_LEN;
    willModifyStore(self);
    NS_DURING
	if (reuse && (handle = [freeInfo findFreeBlock:roundedLength cold:cold inStore:self])) {
	    /* we directly modify current */
	    NSRange		range = {handle + BLOCK_HEADER_LEN, length};
	    unsigned	maxCapacity = roundedLength - BLOCK_HEADER_LEN;
	    unsigned	*nums = NSZoneMalloc(NULL, maxCapacity);
	    unsigned	idx = maxCapacity / WORD_SIZE;
	    [current deserializeInts:nums count:idx atIndex:handle + BLOCK_HEADER_LEN];
	    while (idx--) {
		if (nums[idx]) {
		    [NSException raise:NSInternalInconsistencyException format:@"*** %@ Was expecting 0 at index %d for handle ^%x: %d", NSStringFromSelector(_cmd), idx, handle, nums[idx]];
		}
	    }
	    NSZoneFree(NSZoneFromPointer(nums), nums);
	    [self writeHeader:header inBlock:handle];
	    if (length) [current replaceBytesInRange:range withBytes:[data bytes]];
	    if (padLength) {
		/* we pad, always */
		range.location += range.length; range.length = padLength;
		[current resetBytesInRange:range];
	    }
	    quadLocation = 0;
	    /* we log that for handle, length bytes need to be copied onto the data file before syncing or 'zeroed out' for undoing */
	    if (_debugDataStore) PPLLog(@"Writing in LOG for free block ^%x of length %d", handle, roundedLength);
	    if (maxCapacity) {
		if ([logInfo appendLogFreeAt:handle + BLOCK_HEADER_LEN length:maxCapacity]) {
		    /* The log was truly written (and not just bufferized), therefore an expensive operation has been done, and therefore it is probably worth thinking about remapping */
		    [self tryReflectCurrentOnDiskAndRemap];
		}
		logNeedsTruncate = YES;
	    }
	} else {
	    unsigned	nums[2];
	    handle = [current length];
	    nums[0] = packFirst(header); nums[1] = header.capacity;
	    [current serializeInts:nums count:2];
	    if (length) [current appendData:data];
	    if (padLength) {
		/* we pad, always */
		[current increaseLengthBy:padLength];
	    }
	    quadLocation = 0;
	    lengthToBeAppended += roundedLength;
	    if (lengthToBeAppended >= APPEND_QUANTUM) finishAppendingToDatafd(self);
	    /* The data file was written, therefore an expensive operation has been done, and therefore it is probably worth thinking about remapping */
	    [self tryReflectCurrentOnDiskAndRemap];
	}
	if (trueCapacity) *trueCapacity = roundedLength - BLOCK_HEADER_LEN;
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
    return handle;
}

- (void)writeHeader:(BlockHeader)newHeader inBlock:(unsigned)handle {
    NS_DURING
	writeUnsigned(self, packFirst(newHeader), handle);
	writeUnsigned(self, newHeader.capacity, handle + OFF_CAPACITY);
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)writeUnsigned:(unsigned)val inBlock:(unsigned)handle atIndex:(unsigned)idx {
    unsigned	location = handle + BLOCK_HEADER_LEN + idx * WORD_SIZE;
    NS_DURING
	writeUnsigned(self, val, location);
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)downShiftUnsigned:(NSRange)range inBlock:(unsigned)handle by:(unsigned)shift {
    // this could be optimized by doing it per quad
    if (!range.length) return;
    NS_DURING
	unsigned	location;
	location = handle + BLOCK_HEADER_LEN + range.location * WORD_SIZE;
	while (range.length--) {
	    unsigned	sub = unsignedAt(self, location);
	    writeUnsigned(self, sub, location - shift * WORD_SIZE);
	    location += WORD_SIZE;
	}
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)autoCompactForHeader:(BlockHeader)header inBlock:(unsigned)handle {
    if (!readOnly && datafd && !isInRanges(handle, cleanRanges, numCleanRanges, &hintIndex)) {
	/* We do not do the compaction thing if the type is 0 (meaning free), because the compaction could potentially cause handle to be merged and therefore we would return something obsolete */
	unsigned	limit = ((handle >> logPage) + 1) << logPage;
	unsigned	nextHandle;
	unsigned	curLen = [current length];
	if (limit > curLen) limit = curLen;
	nextHandle = handle + roundUpChunk(header.capacity + BLOCK_HEADER_LEN);
	if (_enableIncrementalCompactionLog) PPLLog(@"readBlockHeaderAndCompact entered with ^%x", handle);
	while (nextHandle < limit) {
	    BlockHeader	nextHeader;
	    if (isInRanges(nextHandle, cleanRanges, numCleanRanges, &hintIndex)) {
		if (_enableIncrementalCompactionLog)  PPLLog(@"readBlockHeaderAndCompact skip ^%x", nextHandle);
		break;
	    }
	    if (_enableIncrementalCompactionLog)  PPLLog(@"readBlockHeaderAndCompact DO ^%x", nextHandle);
	    nextHeader = readHeader(current, nextHandle);
	    if (!nextHeader.type && nextHeader.refCount) {
		PPLLog(@"*** Free handle with refcount %d : ^%x", nextHeader.refCount, nextHandle);
	    }
	    if (!nextHeader.type && !nextHeader.refCount) {
		if (_enableIncrementalCompactionLog)  PPLLog(@"readBlockHeaderAndCompact FOUND FREE ^%x", nextHandle);
		nextHandle = [freeInfo noteFree:nextHandle inStore:self];
	    } else {
		nextHandle += roundUpChunk(nextHeader.capacity + BLOCK_HEADER_LEN);
	    }
	}
	addRangeToRanges(NSMakeRange(handle, nextHandle - handle), &cleanRanges, &numCleanRanges, &hintIndex);
    }
}

/***************	Freeing blocks	***************/

- (void)freeBlock:(unsigned)handle {
    unsigned	count = ([self totalRoundedLengthOfBlock:handle] - BLOCK_HEADER_LEN) / WORD_SIZE;
    unsigned	location = handle + BLOCK_HEADER_LEN;
    unsigned	idx = 0;
    NS_DURING
	if (readOnly) [NSException raise:NSInternalInconsistencyException format:@"*** %@ Can't in read-only store", NSStringFromSelector(_cmd)];
	if (!datafd) [NSException raise:NSInternalInconsistencyException format:@"*** %@ Can't in invalid store", NSStringFromSelector(_cmd)];
	while (idx + 3 < count) {
	    write4Zeros(self, location);
	    location += WORD_SIZE * 4;
	    idx += 4;
	}
	while (idx < count) {
	    writeUnsigned(self, 0, location);
	    location += WORD_SIZE;
	    idx++;
	}
	[freeInfo noteFree:handle inStore:self];
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)mergeFreeBlockWithNext:(unsigned)handle {
    BlockHeader	header1 = [self readHeaderInBlock:handle];
    unsigned		length1 = roundUpChunk(header1.capacity + BLOCK_HEADER_LEN);
    unsigned		nextHandle = handle + length1;
    BlockHeader	header2 = [self readHeaderInBlock:nextHandle];
    unsigned		length2 = roundUpChunk(header2.capacity + BLOCK_HEADER_LEN);
    header1.capacity = length1 + length2 - BLOCK_HEADER_LEN;
    header2.type = 0; header2.capacity = 0;
    if (length1 + length2 < BLOCK_HEADER_LEN) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
    //PPLLog(@"Merging free block ^%x with ^%x", handle, nextHandle);
    [self writeHeader:header1 inBlock:handle];
    [self writeHeader:header2 inBlock:nextHandle];
}

- (unsigned)splitFreeBlockInTwo:(unsigned)handle atIndex:(unsigned)firstBlockLength {
    BlockHeader	header1 = [self readHeaderInBlock:handle];
    unsigned		length1 = roundUpChunk(header1.capacity + BLOCK_HEADER_LEN);
    unsigned		nextHandle = handle + firstBlockLength;
    BlockHeader	header2 = {0, 0, 0};
    if (_debugDataStore) PPLLog(@"Splitting free block ^%x in two at length: %d", handle, firstBlockLength);
    if (firstBlockLength < BLOCK_HEADER_LEN) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
    if (length1 < firstBlockLength + BLOCK_HEADER_LEN) [NSException raise:NSInternalInconsistencyException format:@"*** %@", NSStringFromSelector(_cmd)];
    header1.capacity = firstBlockLength - BLOCK_HEADER_LEN;
    [self writeHeader:header1 inBlock:handle];
    header2.capacity = length1 - firstBlockLength - BLOCK_HEADER_LEN;
    [self writeHeader:header2 inBlock:nextHandle];
    return nextHandle;
}

/***************	Control		***************/

- (void)syncDataOnly {
    unsigned	length = [current length];
    NS_DURING
	if (lengthToBeAppended) finishAppendingToDatafd(self);
	_PPLTestAndRaise(_PPLTestPostAppendingDataBeforeSync);
	[datafd synchronizeFile];
	_PPLTestAndRaise(_PPLTestPostAppendingDataAfterSync);
	if (_debugDataStore) PPLLog(@"DATA synced");
	dataNeedsSync = NO;
	logNeedsTruncate = YES;
	if ([datafd seekToEndOfFile] != length) [NSException raise:NSInternalInconsistencyException format:@"*** PPL: -syncDataOnly: Seek error seek is %d, current length is %d", [datafd offsetInFile], length];
	lastLengthSyncedData = length;
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)tryRemap {
    NSRange	range = {0, [current length]};
    if (readOnly || !datafd) return;
    NS_DURING
	if (lengthToBeAppended) finishAppendingToDatafd(self);
	if (!dataNeedsSync) NS_VOIDRETURN;
	if ([datafd seekToEndOfFile] != range.length) [NSException raise:NSInternalInconsistencyException format:@"*** %@ Seek error seek is %d, current length is %d", NSStringFromSelector(_cmd), [datafd offsetInFile], range.length];
	if (range.length < mapThreshold) NS_VOIDRETURN;
	[self syncDataOnly];
	if (_enableRemap) {
	    if (_checkRemapSame) {
		[datafd seekToFileOffset:range.location];
	        if (![[datafd readDataOfLength:range.length] isEqual:current])
	    	    [NSException raise:NSInternalInconsistencyException format:@"*** PPL: tryRemap different data!"];
	    }
	    [current release];
	    [datafd seekToFileOffset:range.location];
	    current = [[NSMutableData allocWithZone:NULL] initWithData:[datafd readDataOfLength:range.length]];
	    if (_debugDataStore) PPLLog(@"Remapped file for length %d", range.length);
	}
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)reflectCurrentOnDisk {
    _PPLRangeArray	*sorted;
    if (version != CURRENT_VERSION) {
	PPLLog(@"*** Changes to version %d files not allowed", version);
	return;
    }
    if (readOnly || !datafd) return;
    NS_DURING
	sorted = [logInfo flushLogAndRangesOfChanges];
	if (!sorted && !dataNeedsSync) {
	    /* no changes to log, no changes to data, we don't even sync the log */
	    NS_VOIDRETURN;
	}
	if (lengthToBeAppended) finishAppendingToDatafd(self);
	[logInfo appendLogMarkerAndSync:[current length]];
	logNeedsTruncate = YES;
	if (sorted && sorted->count) {
	    /* we copy from current to data file */
	    reflectChangesInData(sorted, current, datafd);
	    if ([datafd seekToEndOfFile] != [current length]) [NSException raise:NSInternalInconsistencyException format:@"*** %@ Seek error seek is %d, current length is %d", NSStringFromSelector(_cmd), [datafd offsetInFile], [current length]];
	    dataNeedsSync = YES;
	}
	if (sorted) FreePPLRangeArray(sorted);
	[self tryRemap];
    NS_HANDLER
	[self noteInvalid];
	[localException raise];
    NS_ENDHANDLER
}

- (void)tryReflectCurrentOnDiskAndRemap {
    unsigned	changes;
    changes = [logInfo sizeOfAllPageChangedAndLogged] + [current length] - lastLengthSyncedData;
    if (changes < NSRealMemoryAvailable()) return;
    [self reflectCurrentOnDisk];
}    

- (void)reallySyncPart1 {
    if (readOnly || !datafd) return;
    [self reflectCurrentOnDisk];
    if (dataNeedsSync) [self syncDataOnly]; /* tryRemap may not have done it! */
}

- (void)reallySyncPart2 {
    if (readOnly || !datafd) return;
    if (logNeedsTruncate) {
	NS_DURING
	    [logInfo truncateLogAndMark:lastLengthSyncedData];
	    logNeedsTruncate = NO;
	NS_HANDLER
	    [self noteInvalid];
	    [localException raise];
	NS_ENDHANDLER
    }
}

- (BOOL)reloadFreeListUntilAbortPredicate:(BOOL (*)(void *))abortion andContext:(void *)context {
    unsigned	handle = FILE_HEADER_LEN;
    if (readOnly) [NSException raise:NSInvalidArgumentException format:@"*** %@ Can't in read-only store", NSStringFromSelector(_cmd)];
    if (!datafd) [NSException raise:NSInvalidArgumentException format:@"*** %@ Can't in invalid store", NSStringFromSelector(_cmd)];
    while (handle < [current length]) {
	BlockHeader	header = [self readHeaderInBlock:handle];
	unsigned	nextHandle = handle + roundUpChunk(header.capacity + BLOCK_HEADER_LEN);
	if (!header.type) {
	    if (header.refCount) {
		PPLLog(@"*** Free handle with refcount %d : ^%x", header.refCount, handle);
	    } else {
		nextHandle = [freeInfo noteFree:handle inStore:self];
	    }
	}
	handle = nextHandle;
	if (abortion(context)) return YES;
    }
    return NO;
}

- (NSData *)contentsAsData {
    return [[current copyWithZone:NULL] autorelease];
}

/***************	Misc		***************/

static NSString *rawDump(NSData *data, id typeProvider) {
    unsigned	magic;
    unsigned	handle;
    id		vers;
    NSMutableString	*res;
    [data getBytes:&magic length:4];
    if (magic == MAGIC_NUMBER_V3) {
	vers = @"V3 store same sex";
    } else if (magic == NSSwapInt(MAGIC_NUMBER_V3)) {
	vers = @"V3 store opposite sex";
    } else {
	vers = @"??? store";
    }
    handle = [data deserializeIntAtIndex:ROOT_OFFSET];
    res = [[NSMutableString allocWithZone:NULL] initWithFormat:@"%@ \tRoot handle: ^%x\n", vers, handle];
    handle = FILE_HEADER_LEN;
    while (handle < [data length]) {
	BlockHeader	header = readHeader(data, handle);
	unsigned	count = header.capacity / WORD_SIZE;
	unsigned	extra = [data deserializeIntAtIndex:handle + BLOCK_HEADER_LEN];
	unsigned	idx = 0;
	BOOL		hasSubs = NO;
	[res appendStoreHandle:handle];
	[res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@":\t"];
	[res replaceCharactersInRange:NSMakeRange([res length], 0) withString:[typeProvider descriptionForHeader:header extra:extra]];
	[res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@"\n"];
	while (idx < count) {
	    if ([typeProvider isSubHandleForHeader:header extra:extra atIndex:idx]) {
		unsigned	sub = [data deserializeIntAtIndex:handle + BLOCK_HEADER_LEN + idx * WORD_SIZE];
		if (!hasSubs) [res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@"\t\t"];
		[res appendStoreHandle:sub];
		hasSubs = YES;
	    }
	    idx++;
	}
	if (hasSubs) [res replaceCharactersInRange:NSMakeRange([res length], 0) withString:@"\n"];
	handle += roundUpChunk(header.capacity + BLOCK_HEADER_LEN);
    }
    return [res autorelease];
}

- (NSString *)description {
    NSString	*res;
    res = [[NSString allocWithZone:NULL] initWithFormat:@"Dump of current:\n%@\n%@",    rawDump(current, typeProvider), freeInfo];
    return [res autorelease];
}

- (void)_rawDumpFile {
    NSString *dpath = dataPath(path);
    NSData *data = [[NSData allocWithZone:NULL] initWithContentsOfFile:dpath];
    if (!data) {
	PPLLog(@"*** Can't read %@", dpath);
        return;
    }
    PPLLog(@"Dump of file %@:\n%@", dpath, rawDump(data, typeProvider));
    [data release];
}

@end

/***************	Store as PPL Data		***************/

@implementation _PPLReadOnlyStore

- initWithPPLData:(NSData *)pplData {
    current = (id)[pplData copyWithZone:NULL];
    return self;
}

- (void)dealloc {
    [current release];
    [super dealloc];
}

@end

/***************	Error / Debug utilities		***********/

static int _PPLRaiseCount = 0;

void _PPLTestAndRaise(NSString *state) {
    if (!_PPLRaiseCount) return; /* not enabled */
    _PPLRaiseCount--;
    if (_PPLRaiseCount) return;
    PPLLog(@"Artifically raised %@", state);
    [NSException raise:_PPLTestException format:@"%@", state];
}

NSRange _PPLRoundUpRange(NSRange range, unsigned rounding) {
    NSRange	rounded = range;
    unsigned	mask = rounding - 1;
    rounded.location &= ~ mask;
    rounded.length += range.location - rounded.location + mask;
    rounded.length &= ~ mask;
    return rounded;
}

/***************	File Utilities		***********/

static BOOL saveOldies = YES;
void _PPLMoveToBackup(NSString *path) {
    if (saveOldies) {
        if (![[NSFileManager defaultManager] movePath:path toPath:[path stringByAppendingString:@"~"] handler:nil]) {
            PPLLog(@"Old file stashed away in %@", [path stringByAppendingString:@"~"]);
	}
    }
}

/***************	Misc		***********/

@implementation NSMutableString (_PPLMutableStringExtras)

- (void)appendStoreHandle:(unsigned)handle {
    [self appendFormat:@"^%x ", handle];
}

@end

/***************	Immutable String	***********/

@implementation _PPLString

- initWithString:(NSString *)str {
    string = [str retain];
    return self;
}

- (unichar)characterAtIndex:(unsigned)loc {
    return [string characterAtIndex:loc];
}

- (void)getCharacters:(unichar *)buffer range:(NSRange)range {
    [string getCharacters:buffer range:range];
}

- (unsigned)length {
    return [string length];
}

- retain {
    _PPLIncrementInternalRefCount(self, &extraRefCount, UINT_MAX, nil);
    return self;
}

- (void)release {
    if (_PPLDecrementInternalRefCountWasZero(self, &extraRefCount, UINT_MAX, nil))
	[self dealloc]; 
}

- (unsigned)retainCount {
    return _PPLInternalRefCount(self, extraRefCount, UINT_MAX);
}

- (void)dealloc {
    [_PPLCache noteDeallocatingProxy:self];
    [string release];
    [super dealloc];
}

@end

/***********		Sorted Int Array		***********/

@implementation _SortedNums

- initWithCapacity:(unsigned)capa {
    max = capa;
    nums = NSZoneMalloc(NULL, max * sizeof(unsigned));
    return self;
}

static unsigned smallestIndexGreaterOrEqual(_SortedNums *self, unsigned num) {
    /* very private */
    unsigned	first = 0;
    unsigned	last = self->count;
    while (first < last) {
	unsigned	half = (first + last) / 2;
	unsigned	numHalf = self->nums[half];
	if (numHalf < num) {
	    first = half + 1;
	} else if (numHalf == num) {
	    return half;
	} else if (last != half + 1) {
	    last = half + 1;
	} else if ((first != half) && (self->nums[first] >= num)) {
	    return first;
	} else {
	    return half;
	}
    }
    return self->count;
}

- (void)addUnsigned:(unsigned)num {
    unsigned	idx = smallestIndexGreaterOrEqual(self, num);
    if (count >= max) {
	max += max + 1;
	nums = NSZoneRealloc(NSZoneFromPointer(nums), nums, max * sizeof(unsigned));
    }
    if (idx != count) {
	unsigned	*source = nums + idx;
	unsigned	*dest = source + 1;
        memmove(dest, source, sizeof(unsigned) * (count - idx));
    }
    nums[idx] = num;
    count++;
}

- (void)removeUnsigned:(unsigned)num {
    unsigned	idx = smallestIndexGreaterOrEqual(self, num);
    if ((idx == count) || (nums[idx] != num)) return;
    count --;
    if (idx != count) {
	unsigned	*dest = nums + idx;
	unsigned	*source = dest + 1;
        memmove(dest, source, sizeof(unsigned) *(count - idx));
    }
}

- (unsigned)smallestGreaterOrEqual:(unsigned)num {
    unsigned	idx = smallestIndexGreaterOrEqual(self, num);
    if (idx == count) return 0;
    return nums[idx];
}

- (unsigned)largestLesser:(unsigned)num {
    unsigned	idx = smallestIndexGreaterOrEqual(self, num);
    if (!idx) return 0;
    return nums[idx - 1];
}

- (unsigned)removeSmallest {
    unsigned	res;
    if (!count) return 0;
    res = nums[0];
    count --;
    if (count) {
	unsigned	*source = nums + 1;
    memmove(nums, source, sizeof(unsigned *) * count);
    }
    return res;
}

- (unsigned)removeLargest {
    if (!count) return 0;
    return nums[--count];
}

- (NSString *)description {
    NSMutableString	*res = [[NSMutableString allocWithZone:NULL] init];
    unsigned		idx = 0;
    while (idx < count) {
	[res appendStoreHandle:nums[idx++]];
    }
    return [res autorelease];
}

- (void)dealloc {
    NSZoneFree(NSZoneFromPointer(nums), nums);
    [super dealloc];
}

@end

/***********		Range Arrays		***********/

_PPLRangeArray *CreatePPLRangeArrayWithZone(unsigned capacity, NSZone *zone) {
    _PPLRangeArray	*ranges = NSZoneMalloc(zone, sizeof(_PPLRangeArray));
    ranges->count = 0;
    ranges->max = capacity;
    ranges->ranges = (capacity) ? NSZoneMalloc(zone, capacity * sizeof(NSRange)) : NULL;
    return ranges;
}

_PPLRangeArray *CreatePPLRangeArray(unsigned capacity) {
    return CreatePPLRangeArrayWithZone(capacity, NULL);
}

void FreePPLRangeArray(_PPLRangeArray *ranges) {
    NSZoneFree(NSZoneFromPointer(ranges->ranges), ranges->ranges);
    NSZoneFree(NSZoneFromPointer(ranges), ranges);
}

static void growPPLRangeArray(_PPLRangeArray *ranges) {
    NSZone	*zone = NSZoneFromPointer(ranges);
    ranges->max += ranges->max + 1;
    ranges->ranges = NSZoneRealloc(zone, ranges->ranges, ranges->max * sizeof(NSRange));
}

static void bubbleUpPPLRangeArray(_PPLRangeArray *ranges, NSRange range, unsigned idx) {
    while (idx) {
	unsigned	parent = (idx-1) >> 1; /* parent */
	NSRange		prange = ranges->ranges[parent];
	if (prange.location <= range.location) break;
	/* swap with parent, i.e. bubble up */
	ranges->ranges[idx] = prange;
	idx = parent;
    }
    ranges->ranges[idx] = range;
}

void AddToPPLRangeArray(_PPLRangeArray *ranges, NSRange range, _PPLRangeArrayAddPolicy policy) {
    switch (policy) {
	case _PPLRangeArrayAppend:	
	    if (ranges->count >= ranges->max) growPPLRangeArray(ranges);
	    ranges->ranges[ranges->count++] = range;
	    break;
	case _PPLRangeArrayAddToHeap:
	    if (ranges->count >= ranges->max) growPPLRangeArray(ranges);
	    bubbleUpPPLRangeArray(ranges, range, ranges->count++);
	    break;
    }
}

static void bubbleDownPPLRangeArray(_PPLRangeArray *ranges, NSRange moved, unsigned parent) {
    unsigned	child;
    while ((child = (parent << 1) + 1) < ranges->count) { /* while there are children */
	/* child is first child */
	NSRange	range = ranges->ranges[child];
	/* we take the smallest of both children */
	if (child + 1 <= ranges->count - 1) {
	    NSRange	range2 = ranges->ranges[child + 1];
	    if (range.location > range2.location) {
		child++;
		range = range2;
	    }
	}
	/* child now is best child; bubble down */
	if (range.location >= moved.location) break;
	ranges->ranges[parent] = range;
	parent = child;
    }
    ranges->ranges[parent] = moved;
}

NSRange RemoveMinimumOfPPLRangeArray(_PPLRangeArray *ranges, _PPLRangeArrayRemoveMinimumPolicy policy) {
    switch (policy) {
	case _PPLRangeArrayRemoveMinimumFromHeap:	{
	    NSRange	top = {0, 0};
	    NSRange	moved;
	    unsigned	parent = 0;
	    if (! ranges->count) return top;
	    top = ranges->ranges[0];
	    ranges->count--;
	    if (! ranges->count) return top;
	    moved = ranges->ranges[ranges->count];
	    bubbleDownPPLRangeArray(ranges, moved, parent);
	    return top;
	}
    }
    return NSMakeRange(0, 0);
}

