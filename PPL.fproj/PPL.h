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
 
    PPL.h
    Persistent Property List
	
    Copyright 1993-1998, Apple Computer, Inc. All rights reserved.
*/

#import <Foundation/NSObject.h>
#import <Foundation/NSDate.h>

@class NSData, NSDictionary, NSMutableDictionary, NSString;
@class _PPLCache, _PPLMutables, _PPLStore;

FOUNDATION_EXPORT NSString * const PPLDidBecomeDirtyNotification;
FOUNDATION_EXPORT NSString * const PPLDidSaveNotification;

@interface PPL : NSObject {
@private
    _PPLStore		*store;
    _PPLMutables	*mutables;
    _PPLCache		*cache;
    unsigned		rootHandle;
    unsigned		version;
    BOOL		readOnly;
    BOOL		isDirty;
    void		*reserved;
}

+ (id)pplWithPath:(NSString *)path create:(BOOL)create readOnly:(BOOL)readOnly;
- (id)initWithPath:(NSString *)path create:(BOOL)create readOnly:(BOOL)readOnly;

+ (id)pplWithPath:(NSString *)path fromPPLData:(NSData *)pplData readOnly:(BOOL)readOnly;
- (id)initWithPath:(NSString *)path fromPPLData:(NSData *)pplData readOnly:(BOOL)readOnly;

+ (NSDictionary *)propertyListWithPPLData:(NSData *)pplData;
- (NSData *)contentsAsData;

- (NSMutableDictionary *)rootDictionary;

- (void)detachFromFile;
- (void)flush;
- (void)pushChangesToDisk;
- (void)save;
- (void)setCacheHalfLife:(NSTimeInterval)halfLife;

@end

