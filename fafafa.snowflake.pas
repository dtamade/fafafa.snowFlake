unit fafafa.snowFlake;

{

# fafafa.snowFlake

```text
   ______   ______     ______   ______     ______   ______
  /\  ___\ /\  __ \   /\  ___\ /\  __ \   /\  ___\ /\  __ \
  \ \  __\ \ \  __ \  \ \  __\ \ \  __ \  \ \  __\ \ \  __ \
   \ \_\    \ \_\ \_\  \ \_\    \ \_\ \_\  \ \_\    \ \_\ \_\
    \/_/     \/_/\/_/   \/_/     \/_/\/_/   \/_/     \/_/\/_/  Studio

```
## 概述

雪花id

-----------------------------------
| Sequence                        | 12 bits
-----------------------------------
| WorkerID                        | 5 bits
-----------------------------------
| DataCenterID                    | 5 bits
-----------------------------------
| Timestamp                       | 41 bits
-----------------------------------
| Unused                          | 1 bits
-----------------------------------


## 声明

转发或者用于自己项目请保留本项目的版权声明,谢谢.

fafafaStudio
Email:dtamade@gmail.com
QQ群:685403987  QQ:179033731

}

{$DEFINE SNOWFLAKE_ENABLE_INLINE}

{$IFDEF FPC}
{$mode ObjFPC}
{$ENDIF}
{$H+}

interface

uses
  Classes, SysUtils, DateUtils,
  fafafa.stopWatch;

const
  USE_UTC = True;
  MAX_YEARS = 69;

  BITS_SEQUENCE = 12;
  BITS_WORKID = 5;
  BITS_DATACENTERID = 5;
  BITS_TIMESTAMP = 41;

  SHIFT_WORKERID = BITS_SEQUENCE;
  SHIFT_DATACENTERID = BITS_SEQUENCE + BITS_WORKID;
  SHIFT_TIMESTAMPLEFT = BITS_SEQUENCE + BITS_WORKID + BITS_DATACENTERID;


  MASK_SEQUENCE = -1 xor (-1 shl BITS_SEQUENCE);
  MAX_WORKERID = -1 xor (-1 shl BITS_WORKID);
  MAX_DATACENTERID = -1 xor (-1 shl BITS_DATACENTERID);

type

  pspinLock = ^spinLock_t;
  spinLock_t = uint32;

  { 创建并初始化原子自旋锁 }
function spinLock_create: pspinLock;

{ 原子自旋锁初始化 }
procedure spinLock_init(aLock: pspinLock);

{ 原子自旋锁加锁 }
procedure spinLock_lock(aLock: pspinLock);

{ 原子自旋锁尝试加锁 }
function spinLock_tryLock(aLock: pspinLock): boolean;

{ 原子自旋锁解锁 }
procedure spinLock_unLock(aLock: pspinLock);

{ 销毁原子自旋锁 }
procedure spinLock_destroy(var aLock: pspinLock);

type

  { ISpinLock }

  ISpinLock = interface
    ['{F94BEAC5-85FA-4621-89EA-AD13193B1571}']

    function GetIsLocking: boolean;
    procedure Lock;
    function TryLock: boolean;
    procedure UnLock;
    property IsLocking: boolean read GetIsLocking;
  end;

  { TSpinLock }

  TSpinLock = class(TInterfacedObject, ISpinLock)
  private
    FLock: pspinLock;
    function GetIsLocking: boolean;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Lock;
    function TryLock: boolean;
    procedure UnLock;

    property IsLocking: boolean read GetIsLocking;
  end;

function createSpinLock: TSpinLock;
function makeSpinLock: ISpinLock;

type
  workerID_t = 0 .. MAX_WORKERID;
  dataCenterID_t = 0 .. MAX_DATACENTERID;

  snowFlakeID_t = int64;

  psnowFlake_t = ^snowFlake_t;

  snowFlake_t = record
    timestamp: int64;
    dataCenterID: dataCenterID_t;
    workerID: workerID_t;
    sequence: UInt16;
  end;


{

snowFlakeID

描述:
  雪花ID装包.

参数:
  - aSequence: ID序列号.
  - aWorkerID 机器ID
  - aDataCenterID 数据中心ID
  - aTimeStamp 相对时间戳,距离生成器基准时间的毫秒数

返回值:
  雪花ID int64整数.

}
function snowFlakeID(aSequence: uint16; aWorkerID:workerID_t; aDataCenterID: dataCenterID_t; aTimeStamp: int64): int64; overload;

{

snowFlakeID

描述:
  雪花ID装包.

参数:
  - aSnowFlake: 雪花ID结构.

返回值:
  雪花ID int64整数.

}
function snowFlakeID(const aSnowFlake: snowFlake_t): int64; overload;

{

snowFlake

描述:
  雪花ID拆包.

参数:
  - aSnowFlakeID: 雪花ID.

返回值:
  雪花ID结构.

}
function snowFlake(aSnowFlakeID: int64): snowFlake_t; overload;

{

snowFlake

描述:
  雪花ID拆包.

参数:
  - aSnowFlakeID: 雪花ID.
  - aSequence ID序列号
  - aWorkerID 机器ID
  - aDataCenterID 数据中心ID
  - aTimeStamp 相对时间戳,距离生成器基准时间的毫秒数

}
procedure snowFlake(aSnowFlakeID: int64; out aSequence: uint16; out aWorkerID:workerID_t; out aDataCenterID: dataCenterID_t; out aTimeStamp: int64); overload;


type

  { c style api}

  psnowFlakeGen_t = ^snowFlakeGen_t;

  snowFlakeGen_t = record
    lock: spinLock_t;
    stopWatch: stopWatch_t;
    workerID: workerID_t;
    dataCenterID: dataCenterID_t;
    epochUnix: int64;
    sequence: uint16;
    lastTimeStamp: int64;
    unixTimestamp: int64;
  end;

{

example:

var
  LGen:psnowFlakeGen_t;
  LID:Int64;
begin
  LGen:=snowFlakeGen_create();
  snowFlakeGen_lock(LGen);  // 同步
  try
    LID:=snowFlakeGen_getNextID(LGen);
  finally
    snowFlakeGen_unLock(LGen);
  end;
  snowFlakeGen_destroy(LGen);
end;

}


{

snowFlakeGen_create

描述:
  通过默认起始基准时间和默认机器以及数据中心ID创建雪花ID生成器.

返回值:
  创建成功的雪花ID生成器指针.

}
function snowFlakeGen_create: psnowFlakeGen_t;

{

snowFlakeGen_create2

描述:
  通过workerID和DataCenterID以及起始基准时间创建雪花ID生成器.

参数:
  - aWorkerID: 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpoch 起始时间基准,TDateTime格式.

返回值:
  创建成功的雪花ID生成器指针.

}
function snowFlakeGen_create2(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime): psnowFlakeGen_t;

{

snowFlakeGen_create3

描述:
  创建雪花ID生成器并设置workerID和DataCenterID以及起始基准时间.

参数:
  - aWorkerID: 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpochUnix 起始时间基准,Unix时间戳格式.

返回值:
  创建成功的雪花ID生成器指针.

}
function snowFlakeGen_create3(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64): psnowFlakeGen_t;

{

snowFlakeGen_destroy

描述:
  释放雪花ID生成器.

}
procedure snowFlakeGen_destroy(var aGen: psnowFlakeGen_t);

{

snowFlakeGen_init

描述:
  通过默认起始基准时间和默认机器以及数据中心ID初始化雪花ID生成器.

参数:
  - aGen 雪花ID生成器指针.

}
procedure snowFlakeGen_init(aGen: psnowFlakeGen_t);

{

snowFlakeGen_init2

描述:
  通过workerID和DataCenterID以及起始基准时间初始化雪花ID生成器.

参数:
  - aGen 雪花ID生成器指针
  - aWorkerID 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpoch 起始时间基准,TDateTime格式.

}
procedure snowFlakeGen_init2(aGen: psnowFlakeGen_t; aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime);

{

snowFlakeGen_init3

描述:
  通过workerID和DataCenterID以及起始基准时间初始化雪花ID生成器.

参数:
  - aGen 雪花ID生成器指针
  - aWorkerID 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpochUnix 起始时间基准,Unix时间戳格式.

}
procedure snowFlakeGen_init3(aGen: psnowFlakeGen_t; aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64);

{ 同步锁定 }
procedure snowFlakeGen_lock(aGen: psnowFlakeGen_t);

{ 尝试同步锁定 }
function snowFlakeGen_tryLock(aGen: psnowFlakeGen_t): boolean;

{ 同步解锁 }
procedure snowflakeGen_unLock(aGen: psnowFlakeGen_t);

{ 获取一个雪花ID }
function snowFlakeGen_getNextID(aGen: psnowFlakeGen_t): int64; {$IFDEF SNOWFLAKE_ENABLE_INLINE}inline;{$ENDIF}

{ 获取机器ID }
function snowFlakeGen_getWorkerID(aGen: psnowFlakeGen_t): workerID_t;

{ 设置机器ID }
procedure snowFlakeGen_setWorkerID(aGen: psnowFlakeGen_t; aWorkerID: workerID_t);

{ 获取数据中心ID }
function snowFlakeGen_getDataCenterID(aGen: psnowFlakeGen_t): dataCenterID_t;

{ 设置数据中心ID }
procedure snowFlakeGen_setDataCenterID(aGen: psnowFlakeGen_t; aDataCenterID: dataCenterID_t);

{ 获取基准起始时间,Unix时间戳格式 }
function snowFlakeGen_getEpochUnix(aGen: psnowFlakeGen_t): int64;

{ 设置基准起始时间,Unix时间戳格式 }
procedure snowFlakeGen_setEpochUnix(aGen: psnowFlakeGen_t; aEpoch: int64);

{ 获取基准起始时间,TDateTime格式 }
function snowFlakeGen_getEpoch(aGen: psnowFlakeGen_t): TDateTime;

{ 设置基准起始时间,TDateTime格式 }
procedure snowFlakeGen_setEpoch(aGen: psnowFlakeGen_t; aEpoch: TDateTime);

{

snowFlakeGen_getTime

描述:
  获取雪花ID的真实创建时间戳.

参数:
  - aGen: 雪花ID生成器指针.
  - aID 雪花ID.

返回值:
  雪花ID的真实创建时间,TDateTime格式.

}
function snowFlakeGen_getTime(aGen: psnowFlakeGen_t; aID: int64): TDateTime;

{

snowFlakeGen_getTimeUnix

描述:
  获取雪花ID的真实创建时间戳.

参数:
  - aGen: 雪花ID生成器指针.
  - aID 雪花ID.

返回值:
  雪花ID的真实创建时间,Unix时间戳格式.

}
function snowFlakeGen_getTimeUnix(aGen: psnowFlakeGen_t; aID: int64): int64;


type

  { ISnowFlakeGen }

  ISnowFlakeGen = interface
    function GetDatacenterId: dataCenterID_t;
    function GetEpoch: TDateTime;
    function GetEpochUnix: int64;
    function GetNextId: int64;
    function GetWorkerID: workerID_t;
    procedure SetDatacenterId(AValue: dataCenterID_t);
    procedure SetEpoch(AValue: TDateTime);
    procedure SetEpochUnix(AValue: int64);
    procedure SetWorkerID(AValue: workerID_t);

    { 设置雪花ID生成器参数 }
    procedure SetSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime); overload;
    procedure SetSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64); overload;

    { 获取雪花ID的真实创建时间戳,TDateTime格式 }
    function GetTime(aSnowFlakeID: int64): TDateTime;

    { 获取雪花ID的真实创建时间戳,Unix时间戳格式 }
    function GetTimeUnix(aSnowFlakeID: int64): int64;

    { 同步锁定 }
    procedure Lock;

    { 尝试同步锁定 }
    function TryLock: boolean;

    { 同步解锁 }
    procedure UnLock;


    { 获取一个雪花ID }
    property NextId: int64 read GetNextId;

    { 获取/设置 机器ID }
    property WorkerID: workerID_t read GetWorkerID write SetWorkerID;

    { 获取/设置 数据中心ID }
    property DatacenterId: dataCenterID_t read GetDatacenterId write SetDatacenterId;

    { 获取/设置 基准起始时间,Unix时间戳格式 }
    property EpochUnix: int64 read GetEpochUnix write SetEpochUnix;

    { 获取/设置 基准起始时间,TDateTime格式}
    property Epoch: TDateTime read GetEpoch write SetEpoch;
  end;

  { TSnowFlakeGen }

  TSnowFlakeGen = class(TInterfacedObject, ISnowFlakeGen)
  private
  class var FInstance: TSnowFlakeGen;
  private
    FGen: snowFlakeGen_t;
    function GetDatacenterId: dataCenterID_t;
    function GetEpoch: TDateTime;
    function GetEpochUnix: int64;
    function GetNextId: int64;
    function GetWorkerID: workerID_t;
    procedure SetDatacenterId(AValue: dataCenterID_t);
    procedure SetEpoch(AValue: TDateTime);
    procedure SetEpochUnix(AValue: int64);
    procedure SetWorkerID(AValue: workerID_t);
  public

    { 获取雪花ID生成器单例实例 }
    class function GetInstance: TSnowFlakeGen;
    class destructor Destroy;

    constructor Create; overload;
    constructor Create(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime); overload;
    constructor Create(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64); overload;

    procedure Lock;
    function TryLock: boolean;
    procedure UnLock;

    { 设置雪花ID生成器参数 }
    procedure SetSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime); overload;
    procedure SetSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64); overload;

    function GetTime(aSnowFlakeID: int64): TDateTime;
    function GetTimeUnix(aSnowFlakeID: int64): int64;

    property NextId: int64 read GetNextId;
    property WorkerID: workerID_t read GetWorkerID write SetWorkerID;
    property DatacenterId: dataCenterID_t read GetDatacenterId write SetDatacenterId;
    property EpochUnix: int64 read GetEpochUnix write SetEpochUnix;
    property Epoch: TDateTime read GetEpoch write SetEpoch;
  end;

{

createSnowFlakeGen

描述:
  通过默认起始基准时间和默认机器以及数据中心ID创建雪花ID生成器对象.

}
function createSnowFlakeGen: TSnowFlakeGen; overload;

{

createSnowFlakeGen

描述:
  通过workerID和DataCenterID以及起始基准时间创建雪花ID生成器对象.

参数:
  - aWorkerID: 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpoch 起始时间基准,TDateTime格式.

}
function createSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime): TSnowFlakeGen; overload;

{

createSnowFlakeGen

描述:
  通过workerID和DataCenterID以及起始基准时间创建雪花ID生成器对象.

参数:
  - aWorkerID: 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpochUnix 起始时间基准,Unix时间戳格式.

}
function createSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64): TSnowFlakeGen; overload;

{

makeSnowFlakeGen

描述:
  通过默认起始基准时间和默认机器以及数据中心ID创建雪花ID生成器接口.

}
function makeSnowFlakeGen: ISnowFlakeGen; overload;

{

makeSnowFlakeGen

描述:
  通过workerID和DataCenterID以及起始基准时间创建雪花ID生成器对象接口.

参数:
  - aWorkerID: 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpoch 起始时间基准,TDateTime格式.

}
function makeSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime): ISnowFlakeGen; overload;

{

makeSnowFlakeGen

描述:
  通过workerID和DataCenterID以及起始基准时间创建雪花ID生成器对象接口.

参数:
  - aWorkerID: 机器ID.
  - aDataCenterID 数据中心ID.
  - aEpochUnix 起始时间基准,Unix时间戳格式.

}
function makeSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64): ISnowFlakeGen; overload;


implementation

const
  SIZE_SNOWFLAKEGEN = sizeof(snowFlakeGen_t);

  ERROR_CLOCK_MOVED_BACKWARDS = 'Clock moved backwards. Refusing to generate id for %d milliseconds';
  ERROR_EPOCH_INVALID = 'Epoch can not be greater than current';
  ERROR_EPOCH_MAX = 'The epoch distance exceeds the maximum value';

var
  INIT_EPOCH_DATE: TDateTime;


function spinLock_create: pspinLock;
begin
  New(Result);
  spinLock_init(Result);
end;

procedure spinLock_init(aLock: pspinLock);
begin
  if (TThread.ProcessorCount = 1) then
    raise Exception.Create('IsSingleCPU,SpinLock Cant Work!');
  aLock^ := 0;
end;

procedure spinLock_lock(aLock: pspinLock);
begin
  while not spinLock_tryLock(aLock) do
  begin
    ThreadSwitch;
    Sleep(1);
  end;
end;

function spinLock_tryLock(aLock: pspinLock): boolean;
begin
  Result := (0 = InterlockedCompareExchange(aLock^, 1, 0));
end;

procedure spinLock_unLock(aLock: pspinLock);
begin
  InterlockedExchange(aLock^, 0);
end;

procedure spinLock_destroy(var aLock: pspinLock);
begin
  Dispose(aLock);
end;

{ TSpinLock }

function TSpinLock.GetIsLocking: boolean;
begin
  Result := (PUInt32(FLock)^ = 1);
end;

constructor TSpinLock.Create;
begin
  inherited Create;
  FLock := spinLock_create();
end;

destructor TSpinLock.Destroy;
begin
  spinLock_destroy(FLock);
  inherited Destroy;
end;

procedure TSpinLock.Lock;
begin
  spinLock_lock(FLock);
end;

function TSpinLock.TryLock: boolean;
begin
  Result := spinLock_tryLock(FLock);
end;

procedure TSpinLock.UnLock;
begin
  spinLock_unLock(FLock);
end;

function createSpinLock: TSpinLock;
begin
  Result := TSpinLock.Create;
end;

function makeSpinLock: ISpinLock;
begin
  Result := createSpinLock;
end;

function snowFlake_getCurrentMs(aSnowFlake: psnowFlakeGen_t): uint64; {$IFDEF SNOWFLAKE_ENABLE_INLINE}inline;{$ENDIF}
begin
  Result := aSnowFlake^.unixTimestamp + stopWatch_getMillisecond(@(aSnowFlake^.stopWatch));
end;

function snowFlake_waitUntilNextTime(aSnowFlake: psnowFlakeGen_t; aTimestamp: uint64): uint64;
begin
  Result := snowFlake_getCurrentMs(aSnowFlake);
  while Result <= aTimestamp do
    Result := snowFlake_getCurrentMs(aSnowFlake);
end;

function snowFlakeGen_create: psnowFlakeGen_t;
begin
  New(Result);
  snowFlakeGen_init(Result);
end;

function snowFlakeGen_create2(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime): psnowFlakeGen_t;
begin
  New(Result);
  snowFlakeGen_init2(Result, aWorkerID, aDataCenterID, aEpoch);
end;

function snowFlakeGen_create3(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64): psnowFlakeGen_t;
begin
  New(Result);
  snowFlakeGen_init3(Result, aWorkerID, aDataCenterID, aEpochUnix);
end;

procedure snowFlakeGen_destroy(var aGen: psnowFlakeGen_t);
begin
  Dispose(aGen);
end;

procedure snowFlakeGen_init(aGen: psnowFlakeGen_t);
begin
  snowFlakeGen_init2(aGen, 1, 1, INIT_EPOCH_DATE);
end;

procedure snowFlakeGen_init2(aGen: psnowFlakeGen_t; aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime);
begin
  snowFlakeGen_init3(aGen, aWorkerID, aDataCenterID, DateTimeToUnix(aEpoch, USE_UTC) * MS_PER_SEC);
end;

procedure snowFlakeGen_init3(aGen: psnowFlakeGen_t; aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64);
begin
  FillChar(aGen^, SIZE_SNOWFLAKEGEN, 0);
  spinLock_init(pspinLock(aGen));
  stopWatch_init_hd(@(aGen^.stopWatch));
  with aGen^ do
  begin
    sequence := 0;
    workerID := aWorkerID;
    dataCenterID := aDataCenterID;
    epochUnix := aEpochUnix;
    lastTimeStamp := -1;
    unixTimestamp := DateTimeToUnix(Now, USE_UTC) * MS_PER_SEC;
  end;
  stopWatch_reset(@(aGen^.stopWatch));
end;

procedure snowFlakeGen_lock(aGen: psnowFlakeGen_t);
begin
  spinLock_lock(pspinLock(aGen));
end;

function snowFlakeGen_tryLock(aGen: psnowFlakeGen_t): boolean;
begin
  Result := spinLock_tryLock(pspinLock(aGen));
end;

procedure snowflakeGen_unLock(aGen: psnowFlakeGen_t);
begin
  spinLock_unLock(pspinLock(aGen));
end;

function snowFlakeGen_getNextID(aGen: psnowFlakeGen_t): int64;
var
  LTimestamp, LOffset: uint64;
begin
  LTimestamp := snowFlake_getCurrentMs(aGen);
  if LTimestamp < aGen^.lastTimeStamp then
  begin
    LOffset := aGen^.lastTimeStamp - LTimestamp;
    if LOffset <= 5 then
    begin
      Sleep(LOffset shr 1);

      LTimestamp := snowFlake_getCurrentMs(aGen);
      if LTimestamp < aGen^.lastTimeStamp then
        raise Exception.CreateFmt(ERROR_CLOCK_MOVED_BACKWARDS, [aGen^.lastTimeStamp - LTimestamp]);
    end;
  end;

  if aGen^.lastTimeStamp = LTimestamp then
  begin
    aGen^.sequence := (aGen^.sequence + 1) and MASK_SEQUENCE;
    if aGen^.sequence = 0 then
      LTimestamp := snowFlake_waitUntilNextTime(aGen, aGen^.lastTimeStamp);
  end
  else
    aGen^.sequence := 0;

  aGen^.lastTimeStamp := LTimestamp;

  Result := snowFlakeID(aGen^.sequence, aGen^.workerID, aGen^.dataCenterID, LTimestamp - aGen^.epochUnix);
end;

function snowFlakeGen_getWorkerID(aGen: psnowFlakeGen_t): workerID_t;
begin
  Result := aGen^.workerID;
end;

procedure snowFlakeGen_setWorkerID(aGen: psnowFlakeGen_t; aWorkerID: workerID_t);
begin
  aGen^.workerID := aWorkerID;
end;

function snowFlakeGen_getDataCenterID(aGen: psnowFlakeGen_t): dataCenterID_t;
begin
  Result := aGen^.dataCenterID;
end;

procedure snowFlakeGen_setDataCenterID(aGen: psnowFlakeGen_t; aDataCenterID: dataCenterID_t);
begin
  aGen^.dataCenterID := aDataCenterID;
end;

function snowFlakeGen_getEpochUnix(aGen: psnowFlakeGen_t): int64;
begin
  Result := aGen^.epochUnix;
end;

procedure snowFlakeGen_setEpochUnix(aGen: psnowFlakeGen_t; aEpoch: int64);
begin
  aGen^.epochUnix := aEpoch;
end;

function snowFlakeGen_getEpoch(aGen: psnowFlakeGen_t): TDateTime;
begin
  Result := UnixToDateTime(aGen^.epochUnix div MS_PER_SEC, USE_UTC);
end;

procedure snowFlakeGen_setEpoch(aGen: psnowFlakeGen_t; aEpoch: TDateTime);
var
  LNow: TDateTime;
begin
  LNow := Now;
  if aEpoch > LNow then
    raise Exception.Create(ERROR_EPOCH_INVALID);

  if YearsBetween(LNow, aEpoch) <= MAX_YEARS then
    aGen^.epochUnix := DateTimeToUnix(aEpoch, USE_UTC) * MS_PER_SEC
  else
    raise Exception.Create(ERROR_EPOCH_MAX);
end;

function snowFlakeGen_getTime(aGen: psnowFlakeGen_t; aID: int64): TDateTime;
begin
  Result := UnixToDateTime(snowFlakeGen_getTimeUnix(aGen, aID), USE_UTC);
end;

function snowFlakeGen_getTimeUnix(aGen: psnowFlakeGen_t; aID: int64): int64;
var
  LSnowFlake: snowFlake_t;
begin
  LSnowFlake := snowFlake(aID);
  Result := (aGen^.epochUnix + LSnowFlake.timestamp) div MS_PER_SEC;
end;

function snowFlakeID(aSequence: uint16; aWorkerID: workerID_t;
  aDataCenterID: dataCenterID_t; aTimeStamp: int64): int64;
begin
  Result := (aTimeStamp shl SHIFT_TIMESTAMPLEFT) or (aDataCenterID shl SHIFT_DATACENTERID) or (aWorkerID shl SHIFT_WORKERID) or aSequence;
end;

function snowFlakeID(const aSnowFlake: snowFlake_t): int64;
begin
  Result := snowFlakeID(aSnowFlake.sequence, aSnowFlake.workerID, aSnowFlake.dataCenterID, aSnowFlake.timestamp);
end;

function snowFlake(aSnowFlakeID: int64): snowFlake_t;
begin
  Result.sequence := aSnowFlakeID and MASK_SEQUENCE;
  Result.workerID := (aSnowFlakeID shr SHIFT_WORKERID) and MAX_WORKERID;
  Result.dataCenterID := (aSnowFlakeID shr SHIFT_DATACENTERID) and MAX_DATACENTERID;
  Result.timestamp := aSnowFlakeID shr SHIFT_TIMESTAMPLEFT;
end;

procedure snowFlake(aSnowFlakeID: int64; out aSequence: uint16; out
  aWorkerID: workerID_t; out aDataCenterID: dataCenterID_t; out
  aTimeStamp: int64);
var
  LSnowFlake: snowFlake_t;
begin
  LSnowFlake := SnowFlake(aSnowFlakeID);
  aSequence := LSnowFlake.sequence;
  aWorkerID := LSnowFlake.workerID;
  aDataCenterID := LSnowFlake.dataCenterID;
  aTimeStamp := LSnowFlake.timestamp;
end;


{ TSnowFlakeGen }

function TSnowFlakeGen.GetDatacenterId: dataCenterID_t;
begin
  Result := snowFlakeGen_getDataCenterID(@FGen);
end;

function TSnowFlakeGen.GetEpoch: TDateTime;
begin
  Result := snowFlakeGen_getEpoch(@FGen);
end;

function TSnowFlakeGen.GetEpochUnix: int64;
begin
  Result := snowFlakeGen_getEpochUnix(@FGen);
end;

function TSnowFlakeGen.GetNextId: int64;
begin
  Result := snowFlakeGen_getNextID(@FGen);
end;

function TSnowFlakeGen.GetWorkerID: workerID_t;
begin
  Result := snowFlakeGen_getWorkerID(@FGen);
end;

procedure TSnowFlakeGen.SetDatacenterId(AValue: dataCenterID_t);
begin
  snowFlakeGen_setDataCenterID(@FGen, AValue);
end;

procedure TSnowFlakeGen.SetEpoch(AValue: TDateTime);
begin
  snowFlakeGen_setEpoch(@FGen, AValue);
end;

procedure TSnowFlakeGen.SetEpochUnix(AValue: int64);
begin
  snowFlakeGen_setEpochUnix(@FGen, AValue);
end;

procedure TSnowFlakeGen.SetWorkerID(AValue: workerID_t);
begin
  snowFlakeGen_setWorkerID(@FGen, AValue);
end;

class function TSnowFlakeGen.GetInstance: TSnowFlakeGen;
begin
  if FInstance = nil then
    FInstance := TSnowFlakeGen.Create;
  Result := FInstance;
end;

class destructor TSnowFlakeGen.Destroy;
begin
  if FInstance <> nil then
    FInstance.Free;
end;

constructor TSnowFlakeGen.Create;
begin
  Create(1, 1, INIT_EPOCH_DATE);
end;

constructor TSnowFlakeGen.Create(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime);
begin
  inherited Create;
  SetSnowFlakeGen(aWorkerID, aDataCenterID, aEpoch);
end;

constructor TSnowFlakeGen.Create(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64);
begin
  inherited Create;
  SetSnowFlakeGen(aWorkerID, aDataCenterID, aEpochUnix);
end;

procedure TSnowFlakeGen.Lock;
begin
  snowFlakeGen_lock(@FGen);
end;

function TSnowFlakeGen.TryLock: boolean;
begin
  Result := snowFlakeGen_tryLock(@FGen);
end;

procedure TSnowFlakeGen.UnLock;
begin
  snowflakeGen_unLock(@FGen);
end;

procedure TSnowFlakeGen.SetSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime);
begin
  snowFlakeGen_init2(@FGen, aWorkerID, aDataCenterID, aEpoch);
end;

procedure TSnowFlakeGen.SetSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64);
begin
  snowFlakeGen_init3(@FGen, aWorkerID, aDataCenterID, aEpochUnix);
end;

function TSnowFlakeGen.GetTime(aSnowFlakeID: int64): TDateTime;
begin
  Result := snowFlakeGen_getTime(@FGen, aSnowFlakeID);
end;

function TSnowFlakeGen.GetTimeUnix(aSnowFlakeID: int64): int64;
begin
  Result := snowFlakeGen_getTimeUnix(@FGen, aSnowFlakeID);
end;

function createSnowFlakeGen: TSnowFlakeGen;
begin
  Result := TSnowFlakeGen.Create;
end;

function createSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime): TSnowFlakeGen;
begin
  Result := TSnowFlakeGen.Create(aWorkerID, aDataCenterID, aEpoch);
end;

function createSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64): TSnowFlakeGen;
begin
  Result := TSnowFlakeGen.Create(aWorkerID, aDataCenterID, aEpochUnix);
end;

function makeSnowFlakeGen: ISnowFlakeGen;
begin
  Result := TSnowFlakeGen.Create;
end;

function makeSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime): ISnowFlakeGen;
begin
  Result := TSnowFlakeGen.Create(aWorkerID, aDataCenterID, aEpoch);
end;

function makeSnowFlakeGen(aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpochUnix: int64): ISnowFlakeGen;
begin
  Result := TSnowFlakeGen.Create(aWorkerID, aDataCenterID, aEpochUnix);
end;

initialization

  { 默认初始epoch }
  INIT_EPOCH_DATE := EncodeDate(2024, 04, 20);

end.
