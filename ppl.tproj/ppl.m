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

    ppl.m
    Persistent property list tool

    Copyright 1993-1998, Apple Computer, Inc. All rights reserved.
*/

#import <Foundation/Foundation.h>
#import <PPL/PPL.h>

@interface PPL (PPLDebugExtras)
- (_PPLStore *)_store;
- (NSString *)_specialDump;
- (void)_rawDumpFile;
@end

@interface PPLCompactor:NSObject {
    FILE	*srcFile, *dstFile, *logFile;
    NSString	*dstPath;
}

- initSrc:(NSString *)src dst:(NSString *)dst;
- (void)compact;

@end

@interface PPLChecker:NSObject {
    FILE *srcFile;
}
- initSrc:(NSString *)src;
- (void)check;
@end


/***************	Definitions			*******/

static BOOL verbose = NO;
static BOOL debug = NO;

@interface NSString (MSRecCopyNSString)
- recursiveMutableCopy;
@end

@interface NSData (MSRecCopyNSData)
- recursiveMutableCopy;
@end

@interface NSArray (MSRecCopyNSArray)
- recursiveMutableCopy;
@end

@interface NSDictionary (MSRecCopyNSDictionary)
- recursiveMutableCopy;
@end

@interface NSObject (NoWarning)
- (void)_rawDumpFile;
@end

/***************	Utilities			*******/

static void usage(void) {
    printf("syntax: ppl dump/load/compact/validate {-verbose}\n"
    	"Utilities for handling PPL files.\n"
	"All arguments are PPL file names:\n"
	"  dump <input>			// dumps in ASCII onto stdout\n"
	"  load <output>		\t// loads stdin (ASCII) into a PPL\n"
	"  compact <input> <output>	// compacts a PPL\n"
	"  check <input>		\t// checks a PPL is valid\n"
	"  rawDump <input>		// dumps all the blocks\n"
    );
}

@implementation NSString (MSRecCopyNSString)
- recursiveMutableCopy {
    return [self copy];
}
@end

@implementation NSData (MSRecCopyNSData)
- recursiveMutableCopy {
    return [self copy];
}
@end

@implementation NSArray (MSRecCopyNSArray)
- recursiveMutableCopy {
    unsigned		count = [self count];
    NSMutableArray *	marray = [[NSMutableArray alloc] initWithCapacity:count];
    unsigned		index = 0;
    while (index < count) {
	id	new = [[self objectAtIndex:index] recursiveMutableCopy];
	[marray addObject:new];
	[new release];
	index++;
    }
    return marray;
}
@end

@implementation NSDictionary (MSRecCopyNSDictionary)
- recursiveMutableCopy {
    NSMutableDictionary *	mdict = [[NSMutableDictionary alloc] initWithCapacity:[self count]];
    id				state = [self keyEnumerator];
    NSString *key;
    while ((key = [state nextObject])) {
	id	new = [[self objectForKey:key] recursiveMutableCopy];
	[mdict setObject:new forKey:key];
	[new release];
    }
    return mdict;
}
@end

/***************	Commands			*******/

void doDump(NSString *input) {
    NSString	*string;
    PPL	*ppl;
    ppl = [[PPL alloc] initWithPath:input create:NO readOnly:YES];
    if (!ppl) exit(-1);
    string = [ppl _specialDump];
    printf("%s\n", [string cString]); // printf for std out
}

void doLoad(NSString *output) {
    id			fd = [NSFileHandle fileHandleWithStandardInput];
    NSString		*string = [[NSString alloc] initWithData:[fd readDataToEndOfFile] encoding:NSNEXTSTEPStringEncoding];
    NSDictionary	*plist;
    PPL		*ppl;
    NSDictionary	*copy;
    if (verbose) NSLog(@"Input read");
    plist = [string propertyList];
    ppl = [[PPL alloc] initWithPath:output create:YES readOnly:NO];
    if (!ppl) exit(-1);
    copy = [plist recursiveMutableCopy];
    [[ppl rootDictionary] addEntriesFromDictionary:copy];
    [ppl save];
    if (verbose) NSLog(@"New PPL saved in %@", output);
    if (debug) {
	BOOL		same;
	[ppl release];
	ppl = [[PPL alloc] initWithPath:output create:NO readOnly:YES];
	if (verbose) NSLog(@"New PPL reread");
	same = [plist isEqual:[ppl rootDictionary]] && [(id)[ppl rootDictionary] isEqual:plist];
	if (verbose) NSLog(@"Comparison yields: %s", (same) ? "YES" : "NO");
    }
}

void doCompact(NSString *input, NSString *output) {
    PPLCompactor *copier;
    copier = [[PPLCompactor alloc] initSrc:input dst:output];
    [copier compact];
    [copier release];
}

void doCheck(NSString *input) {
    PPLChecker *copier;
    copier = [[PPLChecker alloc] initSrc:input];
    [copier check];
    [copier release];
}

void doRawDump(NSString *input) {
    PPL		*ppl;
    ppl = [[PPL alloc] initWithPath:input create:NO readOnly:YES];
    if (!ppl) exit(-1);
    [(id)[ppl _store] _rawDumpFile];
}

/***************	Main			*******/

void main(void) {
    id		pool = [NSAutoreleasePool new];
    NSArray	*args = [[NSProcessInfo processInfo] arguments];
    int		count;
    NSString	*key;
    while (1) {
	unsigned	index;
	index = [args indexOfObject:@"-verbose"];
	if (index != NSNotFound) {
	    NSMutableArray	*new = [args mutableCopy];
	    [new removeObjectAtIndex:index];
	    args = new;
	    verbose = YES;
	    continue;
	}
	index = [args indexOfObject:@"-debug"];
	if (index != NSNotFound) {
	    NSMutableArray	*new = [args mutableCopy];
	    [new removeObjectAtIndex:index];
	    args = new;
	    debug = YES;
	    continue;
	}
	break;
    }
    count = [args count];
    if (count < 2) {
	usage();
	exit(-1);
    }
    key = [args objectAtIndex:1];
    if ([key isEqual:@"dump"] && count == 3) {
	doDump([args lastObject]);
    } else if ([key isEqual:@"load"] && count == 3) {
	doLoad([args lastObject]);
    } else if ([key isEqual:@"compact"] && count == 4) {
	doCompact([args objectAtIndex:2], [args lastObject]);
    } else if ([key isEqual:@"check"] && count == 3) {
	doCheck([args lastObject]);
    } else if ([key isEqual:@"rawDump"] && count == 3) {
	doRawDump([args lastObject]);
    } else {
	usage();
    }
    [pool release];
    exit(0);    
}       


#import <sys/stat.h>
#import <libc.h>
#import <architecture/byte_order.h>
/***************	Definitions			*******/

#define MAGIC_NUMBER_V3		0xbaba0003
#define STORED_FREE		0
#define STORED_FORWARD		1
#define STORED_MARRAY		2
#define STORED_MDICT		3
#define STORED_NSSTRING		4	// NEXTSTEP strings only
#define STORED_IOBJECT		5

#define WORD_SIZE		sizeof(unsigned)
#define BLOCK_HEADER_LEN	(2 * WORD_SIZE)
#define MIN_CHUNK		(4 * WORD_SIZE)	
#define FILE_HEADER_LEN		(4 * WORD_SIZE)
#define ONE_MEG			(1024 * 1024)
#define BUFSIZE			(8192)

#define LOG_MARKER	0xfffffffe	// changing this changes file format

typedef struct {
    unsigned	refCount;	/* limited to 24 bits */
    unsigned	type;		/* limited to 8 bits */
    unsigned	capacity;	/* number of useful bytes */
} NSBlockHeader;

typedef struct {
    unsigned handle;
    NSBlockHeader header;
} MutableInfo;

static NSString *SrcError = @"SrcError";
static NSString *FatalError = @"FatalError";

static inline unsigned roundUp(unsigned length) {
    return ((length + MIN_CHUNK - 1) / MIN_CHUNK) * MIN_CHUNK;
}

static unsigned bucketsForCapacity(unsigned capacity) {
    if ((capacity - WORD_SIZE) % (3 * WORD_SIZE)) NSLog(@"*** *** Damaged dictionary: %d", capacity);
    return (capacity - WORD_SIZE) / (3 * WORD_SIZE);
}

#define IS_DIR(mode) ((mode & S_IFDIR) == S_IFDIR)

/***************	Compact			*******/

@interface Compacter:NSObject {
    FILE *ppl;
    FILE *dest;
    NSMapTable *map;
    NSMapTable *forward;
    NSMapTable *mutables;
    unsigned destHandle;
}
- initPPL:(FILE *)ppl dest:(FILE *)dest;
- (void)compact;
@end

@implementation Compacter

- initPPL:(FILE *)aPPL dest:(FILE *)aDest {
    ppl = aPPL;
    dest = aDest;
    return self;
}

- (unsigned)intAt:(int)pos {
    unsigned val;
    if (fseek(ppl, pos, SEEK_SET) < 0) {
    	[NSException raise:SrcError format:@"seek failed"];
    }
    if (fread(&val, sizeof(unsigned), 1, ppl) != 1) {
    	[NSException raise:SrcError format:@"read failed"];
    }
    return NSSwapLittleIntToHost(val);
}

- (void)intsAt:(int)pos data:(unsigned *)data count:(int)count{
    int i;
    if (fseek(ppl, pos, SEEK_SET) < 0) {
    	[NSException raise:SrcError format:@"seek failed"];
    }
    if (fread(data, sizeof(unsigned), count, ppl) != count) {
    	[NSException raise:SrcError format:@"read failed"];
    }
    for (i = 0; i < count; i++) {
        *data = NSSwapLittleIntToHost(*data);
	data++;
    }
}

- (unsigned)unsignedForBlock:(unsigned)handle atIndex:(unsigned)index {
    unsigned	location = handle + BLOCK_HEADER_LEN + index * WORD_SIZE;
    return [self intAt:location];
} 

- (void)writeUnsigned:(unsigned)val inBlock:(unsigned)handle atIndex:(unsigned)index {
    unsigned location = handle + BLOCK_HEADER_LEN + index * WORD_SIZE;
    unsigned destVal = NSSwapHostIntToLittle(val);
    if (fseek(dest, location, SEEK_SET) < 0) {
    	[NSException raise:FatalError format:@"seek failed in dest"];
    }
    if (fwrite(&destVal, sizeof(unsigned), 1, dest) != 1) {
    	[NSException raise:FatalError format:@"write failed"];
    }
}

- (NSBlockHeader)readHeaderInBlock:(unsigned)handle {
    NSBlockHeader	header;
    unsigned		nums[2];
    [self intsAt:handle data:nums count:2];
    header.refCount = nums[0] >> 8;
    header.type = nums[0] & 0xff;
    header.capacity = nums[1];
    return header;
}

- (void) copyImmutables {
    unsigned handle, nextMarker = ONE_MEG;
    NSBlockHeader header;
    unsigned fileHeader[4];
    char buf[BUFSIZE];
    
    NS_DURING
    fprintf(stdout, "copying immutables");
    fflush(stdout);
    memset(fileHeader, 0, sizeof(fileHeader));
    fileHeader[0] = MAGIC_NUMBER_V3;
    fwrite(fileHeader, sizeof(fileHeader), 1, dest);
    destHandle = handle = FILE_HEADER_LEN;
    for(;;) {
	int realSize;
	header = [self readHeaderInBlock:handle];
	realSize = roundUp(header.capacity + BLOCK_HEADER_LEN);
	switch (header.type) {
	    case STORED_NSSTRING:
	    case STORED_IOBJECT: {
		int remaining = realSize;
		fseek(ppl, handle, SEEK_SET);
		while (remaining) {
		    int bytesToRead = (remaining < BUFSIZE) ? remaining:BUFSIZE;
		    if (fread(buf, sizeof(char), bytesToRead, ppl) != bytesToRead) {
		        [NSException raise:FatalError format:@"error reading immutable"];
		    }
		    if (fwrite(buf, sizeof(char), bytesToRead, dest) != bytesToRead) {
		        [NSException raise:FatalError format:@"error writing immutable"];
		    }
		    remaining -= bytesToRead;
		}
		if (NSMapGet(map, (void *)handle))
		        [NSException raise:FatalError format:@"duplicate mapping"];
		NSMapInsert(map, (void *)handle, (void *)destHandle);
		// This could have been NSMapInsertKnownAbsent() if the
		// exception handling was converted to NSExceptions.
		destHandle += realSize;
		if (ftell(dest) != destHandle)
		        [NSException raise:FatalError format:@"write inconsitency"];
		break;
	    }
	    case STORED_MARRAY:
	    case STORED_MDICT: {
		MutableInfo mutableInfo;
		mutableInfo.handle = handle;
		mutableInfo.header = header;
		NSMapInsert(mutables, (void *)NSCountMapTable(mutables), (void *)memmove(malloc(sizeof(MutableInfo)), & mutableInfo, sizeof(MutableInfo)));
		break;
	    }
	    case STORED_FREE:
		break;
	    case STORED_FORWARD: {
		unsigned newHandle = [self unsignedForBlock:handle atIndex:0];
		if (NSMapGet(forward, (void *)handle))
		        [NSException raise:FatalError format:@"duplicate mapping"];
		NSMapInsert(map, (void *)handle, (void *)newHandle);
		// This could have been NSMapInsertKnownAbsent() if the
		// exception handling was converted to NSExceptions.
		break;
	    }
	    default:
		break;
	}
	handle += realSize;
	if (handle > nextMarker) {
	    fprintf(stdout, ".");
	    fflush(stdout);
	    nextMarker += ONE_MEG;
	}
    }
    NS_HANDLER
        if ([[localException name] isEqualToString:SrcError])
	    [localException raise];
    NS_ENDHANDLER
    fprintf(stdout, " done\n");
    fflush(stdout);
}

- (void) mapOne:(unsigned *)cur {
    unsigned mappedHandle, forwardHandle;
    while ((forwardHandle = (unsigned) NSMapGet(forward, (void *)*cur))) {
	*cur = forwardHandle;
    }
    mappedHandle = (unsigned) NSMapGet(map, (void *)*cur);
    if (!mappedHandle) {
	[NSException raise:FatalError format:@"missing map"];
    }
    *cur = mappedHandle;
}

- (void) mapArray:(unsigned *)buf count:(int)count {
    unsigned *cur, *last = buf + count;

    for (cur = buf; cur < last; cur++) {
	[self mapOne:cur];
    }
}

- (void) mapDict:(unsigned *)buf count:(int)count {
    unsigned *cur = buf;
    int i;
    for (i = 0; i < count; i++) {
	if (cur[1]) {
	    [self mapOne:cur + 1];
	    [self mapOne:cur + 2];
	}
	cur += 3;
    }
}

- (void) swapBytes:(int *)buf count:(int)count {
    int *last = buf + count;
    for (; buf < last; buf++) {
	*buf = NSSwapHostIntToLittle(*buf);
    }
}

- (void) copyMutables {
    int realSize, maxSize = 0, count, nextMarker = ONE_MEG;
    MutableInfo *cur;
    unsigned idx, cnt = NSCountMapTable(mutables);
    unsigned *buf;
    fprintf(stdout, "copying   mutables");
    fflush(stdout);

    for (idx = 0; idx < cnt; idx ++) {
	cur = NSMapGet(mutables, (void *)idx);
	realSize = roundUp(cur->header.capacity + BLOCK_HEADER_LEN);
	if (NSMapGet(map, (void *)cur->handle))
		[NSException raise:FatalError format:@"duplicate mapping"];
	NSMapInsert(map, (void *)cur->handle, (void *)destHandle);
	// This could have been NSMapInsertKnownAbsent() if the
	// exception handling was converted to NSExceptions.
	destHandle += realSize;
	if (realSize > maxSize)
	    maxSize = realSize;
    }
    buf = malloc(maxSize);
    NS_DURING
    for (idx = 0; idx < cnt; idx ++) {
	unsigned destPos;
	cur = NSMapGet(mutables, (void *)idx);
	destPos = (unsigned) NSMapGet(map, (void *)cur->handle);
	realSize = roundUp(cur->header.capacity + BLOCK_HEADER_LEN);
	count = realSize/WORD_SIZE;
	[self intsAt:cur->handle data:(unsigned *)buf count:count];
	switch(cur->header.type) {
	    case STORED_MARRAY:
		[self mapArray:buf + 3 count:buf[2]];
		break;
	    case STORED_MDICT: {
		int numBuckets = bucketsForCapacity(cur->header.capacity);
		[self mapDict:buf + 3 count:numBuckets];
		break;
	    }
	    default:
		[NSException raise:FatalError format:@"mutable data damaged"];
		break;
	}
	[self swapBytes:buf count:count];
	if (ftell(dest) != destPos) 
		[NSException raise:FatalError format:@"write inconsitency"];
	if (fwrite(buf, sizeof(char), realSize, dest) != realSize) {
		[NSException raise:FatalError format:@"error writing mutable"];
	}
	while (cur->handle > nextMarker) {
	    fprintf(stdout, ".");
	    fflush(stdout);
	    nextMarker += ONE_MEG;
	}
    }
    NS_HANDLER
        if ([[localException name] isEqualToString:SrcError])
	    [NSException raise:FatalError format:@"error reading mutable"];
	else
		[localException raise];
    NS_ENDHANDLER
    free(buf);
    fprintf(stdout, " done\n");
    fflush(stdout);
}

- (void) fixRoot {
    unsigned root = [self unsignedForBlock:0 atIndex:1];
    [self mapOne:&root];
    fflush(dest);
    [self writeUnsigned:root inBlock:0 atIndex:1];
    fseek(dest, 0, SEEK_END);
}

- (void)compact {
    unsigned magic;

    fseek(ppl, 0, SEEK_SET);
    fread(&magic, sizeof(unsigned), 1, ppl);
    if (magic != MAGIC_NUMBER_V3) {
	magic = NSSwapInt(magic);
	if (magic != MAGIC_NUMBER_V3) {
	    NSLog(@"*** Bad magic number: 0x%x.\n", magic);
	    return;
	}
    }
    map = NSCreateMapTable(NSNonOwnedPointerMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0);
    forward = NSCreateMapTable(NSNonOwnedPointerMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0);
    mutables = NSCreateMapTable(NSIntMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0);
    NS_DURING
	[self copyImmutables];
	[self copyMutables];
	[self fixRoot];
    NS_HANDLER
        if ([[localException name] isEqualToString:FatalError])
	    NSLog(@"*** fatal error: %@.\n", [localException reason]);
    NS_ENDHANDLER
    NSFreeMapTable(map);
    NSFreeMapTable(forward);
    NSFreeMapTable(mutables);
    map = NULL;
    forward = NULL;
    mutables = NULL;
}

@end

@implementation PPLCompactor
- initSrc:(NSString *)src dst:(NSString *)dst {
    struct stat srcInfo, dstInfo;
    char fullPath[FILENAME_MAX];
    if (stat([src cString], &srcInfo) < 0) {
	NSLog(@"*** Unable to read %s.\n", [src cString]);
	[self dealloc]; 
	return nil;
    }
    if (!IS_DIR(srcInfo.st_mode)) {
	NSLog(@"*** %s is not a ppl.\n", [src cString]);
	[self dealloc]; 
	return nil;
    }
    /* src file */
    sprintf(fullPath, "%s/store", [src cString]);
    srcFile = fopen(fullPath, "r");
    if (!srcFile) {
	NSLog(@"*** Unable to open %s for reading\n", fullPath);
	[self dealloc]; 
	return nil;
    }
    if (stat([dst cString], &dstInfo) < 0) {
#if defined(WIN32)
	if (_mkdir([dst fileSystemRepresentation]) < 0) {
#else
	if (mkdir([dst fileSystemRepresentation], 0777) < 0) {
#endif
	    NSLog(@"*** Unable to create %s.\n", [dst cString]);
	    [self dealloc];
	    return nil;
	}
    } else {
	if (!IS_DIR(dstInfo.st_mode)) {
	    NSLog(@"*** %s is not a ppl.\n", [dst cString]);
	    [self dealloc];
	    return nil;
	}
    }
    /* src file */
    sprintf(fullPath, "%s/store", [dst cString]);
    dstFile = fopen(fullPath, "w+");
    if (!dstFile) {
	NSLog(@"*** Unable to open %s for writing\n", fullPath);
	[self dealloc]; 
	return nil;
    }
    /* log file */
    sprintf(fullPath, "%s/log", [dst cString]);
    logFile = fopen(fullPath, "w+");
    if (!logFile) {
	NSLog(@"*** Unable to open %s for writing\n", fullPath);
	[self dealloc]; 
	return nil;
    }

    dstPath = [dst copy];
    return self;
}

- (void) compact {
    struct stat dstInfo;
    char fullPath[FILENAME_MAX];
    Compacter *compacter = [[Compacter alloc] initPPL:srcFile dest:dstFile];
    unsigned nums[2];

    [compacter compact];
    [compacter release];
    sprintf(fullPath, "%s/store", [dstPath cString]);
    fclose(srcFile);
    if (fflush(dstFile) == EOF) {
	NSLog(@"*** Unable to flush %s.\n", fullPath);
    }
    if (fclose(dstFile) == EOF) {
	NSLog(@"*** Unable to close %s.\n", fullPath);
    }
    stat(fullPath, &dstInfo);
    nums[0] = NSSwapHostIntToLittle(LOG_MARKER);
    nums[1] = NSSwapHostIntToLittle(dstInfo.st_size);
    sprintf(fullPath, "%s/log", [dstPath cString]);
    if (fwrite(nums, sizeof(unsigned), 2, logFile) != 2)
	NSLog(@"*** Unable to write log file %s.\n", fullPath);
    if (fflush(logFile) == EOF) {
	NSLog(@"*** Unable to flush %s.\n", fullPath);
    }
    if (fclose(logFile) == EOF) {
	NSLog(@"*** Unable to close %s.\n", fullPath);
    }
    srcFile = NULL;
    dstFile = NULL;
    logFile = NULL;
}

- (void)dealloc {
    [dstPath release];
    if (srcFile)
	fclose(srcFile);
    if (dstFile)
	fclose(dstFile);
    if (logFile)
	fclose(logFile);
    [super dealloc];
}
@end

/***************	Checker			*******/

typedef struct {
    unsigned handle;
    unsigned refCount;
    NSBlockHeader header;
} RefInfo;

@interface Checker:NSObject {
    FILE *ppl;
    NSMapTable *map;
    NSMapTable *forward;
    NSMapTable *handles;
}
- initPPL:(FILE *)ppl;
- (void)check;
@end

@implementation Checker

- initPPL:(FILE *)aPPL {
    ppl = aPPL;
    return self;
}

- (unsigned)intAt:(int)pos {
    unsigned val;
    if (fseek(ppl, pos, SEEK_SET) < 0) {
    	[NSException raise:SrcError format:@"seek failed"];
    }
    if (fread(&val, sizeof(unsigned), 1, ppl) != 1) {
    	[NSException raise:SrcError format:@"read failed"];
    }
    return NSSwapLittleIntToHost(val);
}

- (void)intsAt:(int)pos data:(unsigned *)data count:(int)count{
    int i;
    if (fseek(ppl, pos, SEEK_SET) < 0) {
    	[NSException raise:SrcError format:@"seek failed"];
    }
    if (fread(data, sizeof(unsigned), count, ppl) != count) {
    	[NSException raise:SrcError format:@"read failed"];
    }
    for (i = 0; i < count; i++) {
        *data = NSSwapLittleIntToHost(*data);
	data++;
    }
}

- (unsigned)unsignedForBlock:(unsigned)handle atIndex:(unsigned)index {
    unsigned	location = handle + BLOCK_HEADER_LEN + index * WORD_SIZE;
    return [self intAt:location];
} 

- (NSBlockHeader)readHeaderInBlock:(unsigned)handle {
    NSBlockHeader	header;
    unsigned		nums[2];
    [self intsAt:handle data:nums count:2];
    header.refCount = nums[0] >> 8;
    header.type = nums[0] & 0xff;
    header.capacity = nums[1];
    return header;
}

- (void) gatherHandles {
    unsigned handle, nextMarker = ONE_MEG;
    NSBlockHeader header;
    
    NS_DURING
    fprintf(stdout, "gathering handles");
    fflush(stdout);
    handle = FILE_HEADER_LEN;
    for(;;) {
	int realSize;
	header = [self readHeaderInBlock:handle];
	realSize = roundUp(header.capacity + BLOCK_HEADER_LEN);
	switch (header.type) {
	    case STORED_FORWARD:
	    case STORED_MARRAY:
	    case STORED_MDICT:
	    case STORED_NSSTRING:
	    case STORED_IOBJECT: {
		RefInfo refInfo;
		refInfo.handle = handle;
		refInfo.refCount = 0;
		refInfo.header = header;
		if (NSMapGet(map, (void *)handle))
			[NSException raise:FatalError format:@"duplicate mapping"];
		NSMapInsert(map, (void *)handle, (void *)NSCountMapTable(handles));
		// This could have been NSMapInsertKnownAbsent() if the
		// exception handling was converted to NSExceptions.
		NSMapInsert(handles, (void *)NSCountMapTable(handles), (void *)memmove(malloc(sizeof(RefInfo)), & refInfo, sizeof(RefInfo)));
		break;
	    }
	    default:
		break;
	}
	handle += realSize;
	if (handle > nextMarker) {
	    fprintf(stdout, ".");
	    fflush(stdout);
	    nextMarker += ONE_MEG;
	}
    }
    NS_HANDLER
        if (![[localException name] isEqualToString:SrcError])
	    [localException raise];
    NS_ENDHANDLER
    fprintf(stdout, " done\n");
    fflush(stdout);
}

- (void) noteReference:(unsigned *)cur {
    unsigned mappedHandle;
    RefInfo *refInfo;
    mappedHandle = (unsigned) NSMapGet(map, (void *)*cur);
    if (!mappedHandle) {
		[NSException raise:FatalError format:@"missing map"];
    }
    refInfo = NSMapGet(handles, (void *) mappedHandle);
    if (! refInfo) {
		[NSException raise:FatalError format:@"missing ref info"];
    }
    refInfo->refCount++;
}

- (void) mapArray:(unsigned *)buf count:(int)count {
    unsigned *cur, *last = buf + count;

    for (cur = buf; cur < last; cur++) {
	[self noteReference:cur];
    }
}

- (void) mapDict:(unsigned *)buf count:(int)count {
    unsigned *cur = buf;
    int i;
    for (i = 0; i < count; i++) {
	if (cur[1]) {
	    [self noteReference:cur + 1];
	    [self noteReference:cur + 2];
	}
	cur += 3;
    }
}

- (void) noteReferences {
    int realSize, maxSize = 0, count, nextMarker = ONE_MEG;
    RefInfo *cur;
    unsigned idx, cnt = NSCountMapTable(handles);
    unsigned *buf;
    fprintf(stdout, "noting references");
    fflush(stdout);

    for (idx = 1; idx < cnt; idx++) {
	cur = NSMapGet(handles, (void *)idx);
	realSize = roundUp(cur->header.capacity + BLOCK_HEADER_LEN);
	if (realSize > maxSize)
	    maxSize = realSize;
    }
    buf = malloc(maxSize);
    if (!buf) {
	NSLog(@"*** Excessive size encountered: %d", maxSize);
	exit(-1);
    }
    NS_DURING
    for (idx = 1; idx < cnt; idx++) {
	cur = NSMapGet(handles, (void *)idx);
	realSize = roundUp(cur->header.capacity + BLOCK_HEADER_LEN);
	count = realSize/WORD_SIZE;
	[self intsAt:cur->handle data:buf count:count];
	switch(cur->header.type) {
	    case STORED_FORWARD:
		[self noteReference:buf + 2];
		break;
	    case STORED_MARRAY:
		[self mapArray:buf + 3 count:buf[2]];
		break;
	    case STORED_MDICT: {
		int numBuckets = bucketsForCapacity(cur->header.capacity);
		[self mapDict:buf + 3 count:numBuckets];
		break;
	    }
 	    case STORED_NSSTRING:
	    case STORED_IOBJECT:
		break;
	    default:
			[NSException raise:FatalError format:@"mutable data damaged"];
		break;
	}
	while (cur->handle > nextMarker) {
	    fprintf(stdout, ".");
	    fflush(stdout);
	    nextMarker += ONE_MEG;
	}
    }
    NS_HANDLER
        if ([[localException name] isEqualToString:SrcError])
	    [NSException raise:FatalError format:@"error reading mutable"];
	else
		[localException raise];
    NS_ENDHANDLER
    free(buf);
    fprintf(stdout, " done\n");
    fflush(stdout);
}

- (void) noteRoot {
    unsigned root = [self unsignedForBlock:0 atIndex:1];
    [self noteReference:&root];
}

- (void) checkReferences {
    RefInfo *cur;
    unsigned idx, cnt = NSCountMapTable(handles);
    for (idx = 1; idx < cnt; idx++) {
	cur = NSMapGet(handles, (void *)idx);
	if (cur->refCount != cur->header.refCount) {
	    NSLog(@"*** Bad ref count, handle = 0x%x, bad ref = %d, real ref = %d, type = %d.\n", cur->handle, cur->header.refCount, cur->refCount, cur->header.type);
	}
    }
}

- (void)check {
    unsigned magic;

    fseek(ppl, 0, SEEK_SET);
    fread(&magic, sizeof(unsigned), 1, ppl);
    if (magic != MAGIC_NUMBER_V3) {
	magic = NSSwapInt(magic);
	if (magic != MAGIC_NUMBER_V3) {
	    NSLog(@"*** Bad magic number: 0x%x.\n", magic);
	    return;
	}
    }
    map = NSCreateMapTable(NSNonOwnedPointerMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0);
    forward = NSCreateMapTable(NSNonOwnedPointerMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0);
    handles = NSCreateMapTable(NSIntMapKeyCallBacks, NSNonOwnedPointerMapValueCallBacks, 0);
    {
	RefInfo info = {0};
	NSMapInsert(handles, (void *)0, (void *)memmove(malloc(sizeof(RefInfo)), &info, sizeof(RefInfo)));
    }
    NS_DURING
	[self gatherHandles];
	[self noteReferences];
	[self noteRoot];
	[self checkReferences];
    NS_HANDLER
        if ([[localException name] isEqualToString:FatalError])
	    NSLog(@"*** fatal error: %@.\n", [localException reason]);
    NS_ENDHANDLER
    NSFreeMapTable(map);
    NSFreeMapTable(forward);
    NSFreeMapTable(handles);
    map = NULL;
    forward = NULL;
    handles = NULL;
}

@end

@implementation PPLChecker
- initSrc:(NSString *)src {
    struct stat srcInfo;
    char fullPath[FILENAME_MAX];
    if (stat([src cString], &srcInfo) < 0) {
	NSLog(@"*** Unable to read %s.\n", [src cString]);
	[self dealloc];
	return nil;
    }
    if (!IS_DIR(srcInfo.st_mode)) {
	NSLog(@"*** %s is not a ppl.\n", [src cString]);
	[self dealloc];
	return nil;
    }
    /* src file */
    sprintf(fullPath, "%s/store", [src cString]);
    srcFile = fopen(fullPath, "r");
    if (!srcFile) {
	NSLog(@"*** Unable to open %s for reading\n", fullPath);
	[self dealloc];
	return nil;
    }
    return self;
}

- (void) check {
    Checker *compacter = [[Checker alloc] initPPL:srcFile];
    [compacter check];
    [compacter release];
    fclose(srcFile);
    srcFile = NULL;
}

- (void)dealloc {
    if (srcFile) fclose(srcFile);
    [super dealloc];
}
@end
