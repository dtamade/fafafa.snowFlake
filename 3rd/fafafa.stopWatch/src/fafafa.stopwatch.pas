unit fafafa.stopWatch;

{

# fafafa.stopWatch

```text
   ______   ______     ______   ______     ______   ______
  /\  ___\ /\  __ \   /\  ___\ /\  __ \   /\  ___\ /\  __ \
  \ \  __\ \ \  __ \  \ \  __\ \ \  __ \  \ \  __\ \ \  __ \
   \ \_\    \ \_\ \_\  \ \_\    \ \_\ \_\  \ \_\    \ \_\ \_\
    \/_/     \/_/\/_/   \/_/     \/_/\/_/   \/_/     \/_/\/_/  Studio

```
## 概述

高精度的计时器

stopWatch 毫秒级精度
hdStopWatch linux纳秒级精度,windows亚纳秒精度(100纳秒)
tscStopWatch 纳秒级精度,硬件TSC计数器


## 声明

转发或者用于自己项目请保留本项目的版权声明,谢谢.

fafafaStudio
Email:dtamade@gmail.com
QQ群:685403987  QQ:179033731

}


{$DEFINE STOPWATCH_ENABLE_RDTSCP}// RDTSCP会比CPUID+RDTSC开销小点.

{$mode ObjFPC}{$H+}

interface

type

  pstopWatch_t = ^stopWatch_t;

  stopWatch_tickCb_t = function: uint64;

  stopWatch_t = record
    resolution: uint64;
    isRunning, isPause: boolean;
    startTick, stopTick, pauseTick: uint64;
    doTick: stopWatch_tickCb_t;
  end;

  { 纳秒,微秒,毫秒,秒,分,小时,天 }
  timeUnit_t = (TU_NanoSecond, TU_MicroSecond, TU_MilliSecond, TU_Second, TU_Minute, TU_Hour, TU_Day);

  pelapsed_t = ^elapsed_t;

  elapsed_t = record
    TimeUnit: timeUnit_t;
    Elapsed: double;
  end;

  { 创建普通定时器 }
function stopWatch_create(aRunNow: boolean): pstopWatch_t;

{ 创建高精度定时器 }
function stopWatch_create_hd(aRunNow: boolean): pstopWatch_t;

{ 创建时间戳计数器(TSC)定时器 }
function stopWatch_create_tsc(aRunNow: boolean): pstopWatch_t;

{ 销毁定时器，释放其资源 }
procedure stopWatch_destroy(aWatch: pstopwatch_t);

{ 初始化普通定时器 }
procedure stopWatch_init(aWatch: pstopWatch_t);

{ 初始化高精度定时器 }
procedure stopWatch_init_hd(aWatch: pstopWatch_t);

{ 初始化时间戳计数器(TSC)定时器 }
procedure stopWatch_init_tsc(aWatch: pstopWatch_t);

{ 校准时间戳计数器(TSC)的频率 }
procedure stopWatch_tscCalibrate;

{ 启动定时器，开始计时 }
procedure stopWatch_start(aWatch: pstopWatch_t);

{ 暂停定时器，停止计时，但并不清除定时器的计时数据 }
procedure stopWatch_pause(aWatch: pstopWatch_t);

{ 恢复定时器，从暂停状态返回运行状态，继续计时 }
procedure stopWatch_resume(aWatch: pstopWatch_t);

{ 停止定时器，清除定时器的计时数据 }
procedure stopWatch_stop(aWatch: pstopWatch_t);

{ 重置定时器，清除定时器的计时数据，并将定时器立即重新计时 }
procedure stopWatch_reset(aWatch: pstopWatch_t);

{ 检查定时器是否正在运行 }
function stopWatch_isRunning(aWatch: pstopWatch_t): boolean;

{ 检查定时器是否处于暂停状态 }
function stopWatch_isPause(aWatch: pstopWatch_t): boolean;

{ 获取定时器的时钟分辨率 }
function stopWatch_getResolution(aWatch: pstopWatch_t): uint64;

{ 获取定时器自启动以来的经过的时间，返回值为包含详细时间信息的结构体 }
function stopWatch_getElapsed(aWatch: pstopWatch_t): elapsed_t;

{ 获取从启动到目前为止的Tick计数 }
function stopWatch_getTick(aWatch: pstopWatch_t): uint64;

{ 获取从启动到目前为止的纳秒数 浮点数 }
function stopWatch_getNanoSecondF(aWatch: pstopWatch_t): double;

{ 获取从启动到目前为止的纳秒数 整数 }
function stopWatch_getNanoSecond(aWatch: pstopWatch_t): uint64;

{ 获取从启动到目前为止的微秒数 浮点数 }
function stopWatch_getMicroSecondF(aWatch: pstopWatch_t): double;

{ 获取从启动到目前为止的微秒数 整数 }
function stopWatch_getMicroSecond(aWatch: pstopWatch_t): uint64;

{ 获取从启动到目前为止的毫秒数 浮点数 }
function stopWatch_getMillisecondF(aWatch: pstopWatch_t): double;

{ 获取从启动到目前为止的毫秒数 整数 }
function stopWatch_getMillisecond(aWatch: pstopWatch_t): uint64;

{ 获取从启动到目前为止的秒数 浮点数 }
function stopWatch_getSecondF(aWatch: pstopWatch_t): double;

{ 获取从启动到目前为止的秒数 整数 }
function stopWatch_getSecond(aWatch: pstopWatch_t): uint64;

{ 获取从启动到目前为止的分钟数 浮点数 }
function stopWatch_getMinuteF(aWatch: pstopWatch_t): double;

{ 获取从启动到目前为止的分钟数 整数 }
function stopWatch_getMinute(aWatch: pstopWatch_t): uint64;

{ 获取从启动到目前为止的小时数 浮点数 }
function stopWatch_getHourF(aWatch: pstopWatch_t): double;

{ 获取从启动到目前为止的小时数 整数 }
function stopWatch_getHour(aWatch: pstopWatch_t): uint64;

{ 获取从到目前为止的天数 浮点数 }
function stopWatch_getDayF(aWatch: pstopWatch_t): double;

{ 获取从到目前为止的天数 整数 }
function stopWatch_getDay(aWatch: pstopWatch_t): uint64;

type

  { IStopWatch }

  IStopWatch = interface
    ['{AAA0371E-F42A-415D-AA6C-D44108D21585}']
    function GetIsPause: boolean;
    function GetIsRunning: boolean;
    function GetResolution: uint64;

    { 开始计时 }
    procedure Start;

    { 暂停计时 }
    procedure Pause;

    { 恢复计时 }
    procedure Resume;

    { 停止计时 }
    procedure Stop;

    { 重置定时器，清除定时器的计时数据，并将定时器立即重新计时 }
    procedure Reset;

    { 获取从启动到目前为止的Tick计数 }
    function GetTick: uint64;

    { 获取定时器自启动以来的经过的时间，返回值为包含详细时间信息的结构体 }
    function GetElapsed: elapsed_t;

    { 获取从启动到目前为止的纳秒数 浮点数 }
    function GetNanoSecondF: double;

    { 获取从启动到目前为止的纳秒数 整数 }
    function GetNanoSecond: uint64;

    { 获取从启动到目前为止的微秒数 浮点数 }
    function GetMicroSecondF: double;

    { 获取从启动到目前为止的微秒数 整数 }
    function GetMicroSecond: uint64;

    { 获取从启动到目前为止的毫秒数 浮点数 }
    function GetMillisecondF: double;

    { 获取从启动到目前为止的毫秒数 整数 }
    function GetMillisecond: uint64;

    { 获取从启动到目前为止的秒数 浮点数 }
    function GetSecondF: double;

    { 获取从启动到目前为止的秒数 整数 }
    function GetSecond: uint64;

    { 获取从启动到目前为止的分钟数 浮点数 }
    function GetMinuteF: double;

    { 获取从启动到目前为止的分钟数 整数 }
    function GetMinute: uint64;

    { 获取从启动到目前为止的小时数 浮点数 }
    function GetHourF: double;

    { 获取从启动到目前为止的小时数 整数 }
    function GetHour: uint64;

    { 获取从到目前为止的天数 浮点数 }
    function GetDayF: double;

    { 获取从到目前为止的天数 整数 }
    function GetDay: uint64;

    { 是否在执行中 }
    property IsRunning: boolean read GetIsRunning;

    { 是否暂停计时 }
    property IsPause: boolean read GetIsPause;

    { 精度分辨率 计时器的分辨率代表每秒的Tick数量 }
    property Resolution: uint64 read GetResolution;
  end;

  { TStopWatch }

  TStopWatch = class(TInterfacedObject, IStopWatch)
  private
    FWatch: pstopWatch_t;
    function GetIsPause: boolean;
    function GetIsRunning: boolean;
    function GetResolution: uint64;
  protected
    function CreateWatch(aRunNow: boolean): pstopWatch_t; virtual;
  public
    constructor Create; virtual; overload;
    constructor Create(aRunNow: boolean); overload;
    destructor Destroy; override;

    procedure Start;
    procedure Pause;
    procedure Resume;
    procedure Stop;
    procedure Reset;

    function GetTick: uint64;
    function GetElapsed: elapsed_t;
    function GetNanoSecondF: double;
    function GetNanoSecond: uint64;
    function GetMicroSecondF: double;
    function GetMicroSecond: uint64;
    function GetMillisecondF: double;
    function GetMillisecond: uint64;
    function GetSecondF: double;
    function GetSecond: uint64;
    function GetMinuteF: double;
    function GetMinute: uint64;
    function GetHourF: double;
    function GetHour: uint64;
    function GetDayF: double;
    function GetDay: uint64;

    property IsRunning: boolean read GetIsRunning;
    property IsPause: boolean read GetIsPause;
    property Resolution: uint64 read GetResolution;
  end;

  { 构造秒表对象 aRunNow 立即执行 }
function createStopWatch(aRunNow: boolean): TStopWatch; overload;
function createStopWatch: TStopWatch; overload;

{ 构造秒表接口 aRunNow 立即执行 }
function makeStopWatch(aRunNow: boolean): IStopWatch; overload;
function makeStopWatch: IStopWatch; overload;

type

  { THDStopWatch }

  THDStopWatch = class(TStopWatch)
  protected
    function CreateWatch(aRunNow: boolean): pstopWatch_t; override;
  end;

  { 构造高清秒表对象 aRunNow 立即执行 }
function createHDStopWatch(aRunNow: boolean): THDStopWatch; overload;
function createHDStopWatch: THDStopWatch; overload;

{ 构造高清秒表接口 aRunNow 立即执行 }
function makeHDStopWatch(aRunNow: boolean): IStopWatch; overload;
function makeHDStopWatch: IStopWatch; overload;

type

  { ITSCStopWatch }

  ITSCStopWatch = interface(IStopWatch)
    ['{1780418D-F1AD-4C91-8ED2-4FD7874F492B}']

    { 校正CPU }
    procedure Calibrate;
  end;

  { TTSCStopWatch }

  TTSCStopWatch = class(TStopWatch, ITSCStopWatch)
  protected
    function CreateWatch(aRunNow: boolean): pstopWatch_t; override;
  public
    procedure Calibrate;
  end;

  { 构造TSC硬件计时器高清秒表对象 aRunNow 立即执行 }
function createTSCStopWatch(aRunNow: boolean): TTSCStopWatch; overload;
function createTSCStopWatch: TTSCStopWatch; overload;

{ 构造TSC硬件计时器高清秒表接口 aRunNow 立即执行 }
function makeTSCStopWatch(aRunNow: boolean): ITSCStopWatch; overload;
function makeTSCStopWatch: ITSCStopWatch; overload;

const

  MS_PER_SEC = 1000;
  MICROSEC_PER_SEC = 1000000;
  NS_PER_SEC = 1000000000;
  PS_PER_SEC = 1000000000000;


  { 时间单位转为字符串 }
function TimeUnitToStr(aUnit: timeUnit_t): string;

function tickToNanoSecond(aTick, aResolution: uint64): double; //inline;
function tickToMicroSecond(aTick, aResolution: uint64): double; //inline;
function tickToMillisecond(aTick, aResolution: uint64): double; //inline;
function tickToSecond(aTick, aResolution: uint64): double; inline;
function tickToMinute(aTick, aResolution: uint64): double; inline;
function tickToHour(aTick, aResolution: uint64): double; inline;
function tickToDay(aTick, aResolution: uint64): double; inline;

function GetTickCountHD: uint64;
function GetHDResolution: uint64;

procedure USleep(aUS: uint64);

implementation

{$IfDef MSWINDOWS}

uses
  Windows;

function GetTickCountHD: uint64;
begin
  if not QueryPerformanceCounter(int64(Result)) then
    Result := GetTickCount64;
end;

function GetHDResolution: uint64;
begin
  if not QueryPerformanceFrequency(int64(Result)) then
    Result := MS_PER_SEC;
end;

{$ELSE}
  uses
  sysutils,
  BaseUnix,
  {$if DEFINED(LINUX)}
   linux
  {$ELSEIF DEFINED(FREEBSD)}
    FreeBSD
  {$ENDIF};

{ TfafafaHDWatch }

function GetTickCountHD: uint64;
var
  LTP: timespec;
begin
  clock_gettime(CLOCK_MONOTONIC,@LTP);
  Result:=(LTP.tv_sec*NS_PER_SEC)+LTP.tv_nsec;
end;

function GetHDResolution:UInt64;
var
  LRes: timespec;
begin
  if clock_getres(CLOCK_MONOTONIC,@LRes)=0 then
    Result:=NS_PER_SEC
  else
    Result:=MS_PER_SEC;
end;

{$ENDIF}

var

  TSC_TICK_PER_NS: double;

  {$AsmMode INTEL }
function ReadTSC: uint64; assembler;
asm
         {$IfDEF CPU64}
    XOR   RAX, RAX
    {$IFDEF STOPWATCH_ENABLE_RDTSCP}
      RDTSCP
    {$ELSE}
      PUSH  RBX
      CPUID
      POP   RBX
      RDTSC
    {$ENDIF}
    SHL   RDX, 32
    OR    RAX, RDX
         {$else}
         XOR     EAX, EAX
         {$IFDEF STOPWATCH_ENABLE_RDTSCP}
         RDTSCP
         {$ELSE}
      PUSH    EBX
      CPUID
      POP     EBX
      RDTSC
         {$ENDIF}
         {$endif}
end;

procedure USleep(aUS: uint64);
var
  LEnd: uint64;
begin
  LEnd := GetTickCountHD + ((aUS * GetHDResolution) div MICROSEC_PER_SEC);
  while GetTickCountHD < LEnd do
    ThreadSwitch;
end;

procedure stopWatch_tscCalibrate;
var
  LStartTick, LEndTick, LTSCStartTick, LTSCEndTick, LDurationNS, LDurationTick: uint64;
begin
  LStartTick := GetTickCountHD;
  LTSCStartTick := ReadTSC;
  Sleep(4);
  LTSCEndTick := ReadTSC;
  LEndTick := GetTickCountHD;

  LDurationNS := ((LEndTick - LStartTick) * NS_PER_SEC) div GetHDResolution;
  LDurationTick := LTSCEndTick - LTSCStartTick;
  TSC_TICK_PER_NS := (LDurationTick) / LDurationNS;
end;

function doStopWatchTSCTick: uint64;
begin
  Result := Round(ReadTSC / TSC_TICK_PER_NS);
end;

function createStopWatch(aRunNow: boolean): TStopWatch;
begin
  Result := TStopWatch.Create(aRunNow);
end;

function createStopWatch: TStopWatch;
begin
  Result := TStopWatch.Create;
end;

function makeStopWatch(aRunNow: boolean): IStopWatch;
begin
  Result := TStopWatch.Create(aRunNow);
end;

function makeStopWatch: IStopWatch;
begin
  Result := TStopWatch.Create;
end;

function createHDStopWatch(aRunNow: boolean): THDStopWatch;
begin
  Result := THDStopWatch.Create(aRunNow);
end;

function createHDStopWatch: THDStopWatch;
begin
  Result := THDStopWatch.Create;
end;

function makeHDStopWatch(aRunNow: boolean): IStopWatch;
begin
  Result := THDStopWatch.Create(aRunNow);
end;

function makeHDStopWatch: IStopWatch;
begin
  Result := THDStopWatch.Create;
end;

function createTSCStopWatch(aRunNow: boolean): TTSCStopWatch;
begin
  Result := TTSCStopWatch.Create(aRunNow);
end;

function createTSCStopWatch: TTSCStopWatch;
begin
  Result := TTSCStopWatch.Create;
end;

function makeTSCStopWatch(aRunNow: boolean): ITSCStopWatch;
begin
  Result := TTSCStopWatch.Create(aRunNow);
end;

function makeTSCStopWatch: ITSCStopWatch;
begin
  Result := TTSCStopWatch.Create;
end;

function TimeUnitToStr(aUnit: timeUnit_t): string;
begin
  case aUnit of
    TU_NanoSecond: Result := 'ns';
    TU_MicroSecond: Result := (#166#204#115);//#166#204#115; // μs
    TU_MilliSecond: Result := 'ms';
    TU_Second: Result := 's';
    TU_Minute: Result := 'm';
    TU_Hour: Result := 'h';
    TU_Day: Result := 'd';
  end;
end;

function tickToNanoSecond(aTick, aResolution: uint64): double;
begin
  Result := (aTick * NS_PER_SEC) / aResolution;
end;

function tickToMicroSecond(aTick, aResolution: uint64): double;
begin
  Result := (aTick * MICROSEC_PER_SEC) / aResolution;
end;

function tickToMillisecond(aTick, aResolution: uint64): double;
begin
  Result := (aTick * MS_PER_SEC) / aResolution;
end;

function tickToSecond(aTick, aResolution: uint64): double;
begin
  Result := tickToMillisecond(aTick, aResolution) / 1000;
end;

function tickToMinute(aTick, aResolution: uint64): double;
begin
  Result := tickToSecond(aTick, aResolution) / 60;
end;

function tickToHour(aTick, aResolution: uint64): double;
begin
  Result := tickToMinute(aTick, aResolution) / 60;
end;

function tickToDay(aTick, aResolution: uint64): double;
begin
  Result := tickToHour(aTick, aResolution) / 24;
end;

function stopWatch_create(aRunNow: boolean): pstopWatch_t;
begin
  New(Result);
  stopWatch_init(Result);
  if aRunNow then
    stopWatch_start(Result);
end;

function stopWatch_create_hd(aRunNow: boolean): pstopWatch_t;
begin
  New(Result);
  stopWatch_init_hd(Result);
  if aRunNow then
    stopWatch_start(Result);
end;

function stopWatch_create_tsc(aRunNow: boolean): pstopWatch_t;
begin
  New(Result);
  stopWatch_init_tsc(Result);
  if aRunNow then
    stopWatch_start(Result);
end;

procedure stopWatch_destroy(aWatch: pstopwatch_t);
begin
  Dispose(aWatch);
end;

function doStopWatchTick: uint64;
begin
  Result := GetTickCount64;
end;

procedure stopWatch_init(aWatch: pstopWatch_t);
begin
  FillChar(aWatch^, SizeOf(stopWatch_t), 0);
  aWatch^.resolution := MS_PER_SEC;
  aWatch^.doTick := @doStopWatchTick;
end;

procedure stopWatch_init_hd(aWatch: pstopWatch_t);
begin
  FillChar(aWatch^, SizeOf(stopWatch_t), 0);
  aWatch^.resolution := GetHDResolution;
  aWatch^.doTick := @GetTickCountHD;
end;

procedure stopWatch_init_tsc(aWatch: pstopWatch_t);
begin
  FillChar(aWatch^, SizeOf(stopWatch_t), 0);
  aWatch^.resolution := NS_PER_SEC;
  aWatch^.doTick := @doStopWatchTSCTick;
  stopWatch_tscCalibrate;
end;

procedure stopWatch_start(aWatch: pstopWatch_t);
begin
  if not aWatch^.isRunning then
  begin
    if aWatch^.isPause then
      stopWatch_resume(aWatch)
    else
    begin
      aWatch^.startTick := aWatch^.doTick();
      aWatch^.isRunning := True;
    end;
  end;
end;

procedure stopWatch_pause(aWatch: pstopWatch_t);
begin
  if aWatch^.isRunning then
  begin
    aWatch^.pauseTick := aWatch^.doTick();
    aWatch^.isPause := True;
    aWatch^.isRunning := False;
  end;
end;

procedure stopWatch_resume(aWatch: pstopWatch_t);
begin
  if aWatch^.isPause and (not aWatch^.isRunning) then
  begin
    Inc(aWatch^.startTick, aWatch^.doTick() - aWatch^.pauseTick);
    aWatch^.isRunning := True;
  end;
end;

procedure stopWatch_stop(aWatch: pstopWatch_t);
begin
  if (aWatch^.isRunning) or (aWatch^.isPause) then
  begin
    aWatch^.stopTick := aWatch^.doTick();
    aWatch^.isRunning := False;
    aWatch^.isPause := False;
  end;
end;

procedure stopWatch_reset(aWatch: pstopWatch_t);
begin
  aWatch^.isRunning := False;
  aWatch^.isPause := False;
  stopWatch_start(aWatch);
end;

function stopWatch_isRunning(aWatch: pstopWatch_t): boolean;
begin
  Result := aWatch^.isRunning;
end;

function stopWatch_isPause(aWatch: pstopWatch_t): boolean;
begin
  Result := aWatch^.isPause;
end;

function stopWatch_getResolution(aWatch: pstopWatch_t): uint64;
begin
  Result := aWatch^.resolution;
end;

function stopWatch_getElapsed(aWatch: pstopWatch_t): elapsed_t;
var
  LTick: uint64;
begin
  LTick := stopWatch_getTick(aWatch);
  with Result do
  begin
    Elapsed := tickToNanoSecond(LTick, aWatch^.resolution);
    if Elapsed < 1000 then
    begin
      TimeUnit := TU_NanoSecond;
      Exit;
    end;

    Elapsed := tickToMicroSecond(LTick, aWatch^.resolution);
    if Elapsed < 1000 then
    begin
      TimeUnit := TU_MicroSecond;
      Exit;
    end;

    Elapsed := tickToMillisecond(LTick, aWatch^.resolution);
    if Elapsed < 1000 then
    begin
      TimeUnit := TU_MilliSecond;
      Exit;
    end;

    Elapsed := tickToSecond(LTick, aWatch^.resolution);
    if Elapsed < 60 then
    begin
      TimeUnit := TU_Second;
      Exit;
    end;

    Elapsed := tickToMinute(LTick, aWatch^.resolution);
    if Elapsed < 60 then
    begin
      TimeUnit := TU_Minute;
      Exit;
    end;

    Elapsed := tickToHour(LTick, aWatch^.resolution);
    if Elapsed < 24 then
    begin
      TimeUnit := TU_Hour;
      Exit;
    end;

    Elapsed := tickToDay(LTick, aWatch^.resolution);
    TimeUnit := TU_Day;
  end;
end;

function stopWatch_getTick(aWatch: pstopWatch_t): uint64;
var
  LElapsed: uint64;
begin
  if aWatch^.isRunning then
    LElapsed := aWatch^.doTick()
  else if aWatch^.IsPause then
    LElapsed := aWatch^.pauseTick
  else
    LElapsed := aWatch^.stopTick;

  Result := LElapsed - aWatch^.startTick;
end;

function stopWatch_getNanoSecondF(aWatch: pstopWatch_t): double;
begin
  Result := tickToNanoSecond(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getNanoSecond(aWatch: pstopWatch_t): uint64;
begin
  Result := Round(stopWatch_getNanoSecondF(aWatch));
end;

function stopWatch_getMicroSecondF(aWatch: pstopWatch_t): double;
begin
  Result := tickToMicroSecond(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getMicroSecond(aWatch: pstopWatch_t): uint64;
begin
  Result := round(stopWatch_getMicroSecondF(aWatch));
end;

function stopWatch_getMillisecondF(aWatch: pstopWatch_t): double;
begin
  Result := tickToMillisecond(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getMillisecond(aWatch: pstopWatch_t): uint64;
begin
  Result := round(stopWatch_getMillisecondF(aWatch));
end;

function stopWatch_getSecondF(aWatch: pstopWatch_t): double;
begin
  Result := tickToSecond(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getSecond(aWatch: pstopWatch_t): uint64;
begin
  Result := Round(stopWatch_getSecondF(aWatch));
end;

function stopWatch_getMinuteF(aWatch: pstopWatch_t): double;
begin
  Result := tickToMinute(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getMinute(aWatch: pstopWatch_t): uint64;
begin
  Result := Round(stopWatch_getMinuteF(aWatch));
end;

function stopWatch_getHourF(aWatch: pstopWatch_t): double;
begin
  Result := tickToHour(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getHour(aWatch: pstopWatch_t): uint64;
begin
  Result := Round(stopWatch_getHourF(aWatch));
end;

function stopWatch_getDayF(aWatch: pstopWatch_t): double;
begin
  Result := tickToDay(stopWatch_getTick(aWatch), aWatch^.resolution);
end;

function stopWatch_getDay(aWatch: pstopWatch_t): uint64;
begin
  Result := Round(stopWatch_getDayF(aWatch));
end;

{ TStopWatch }

function TStopWatch.GetIsPause: boolean;
begin
  Result := stopWatch_isPause(FWatch);
end;

function TStopWatch.GetIsRunning: boolean;
begin
  Result := stopWatch_isRunning(FWatch);
end;

function TStopWatch.GetResolution: uint64;
begin
  Result := stopWatch_getResolution(FWatch);
end;

function TStopWatch.CreateWatch(aRunNow: boolean): pstopWatch_t;
begin
  Result := stopWatch_create(aRunNow);
end;

constructor TStopWatch.Create;
begin
  Create(False);
end;

constructor TStopWatch.Create(aRunNow: boolean);
begin
  inherited Create;
  FWatch := CreateWatch(aRunNow);
end;

destructor TStopWatch.Destroy;
begin
  stopWatch_destroy(FWatch);
  inherited Destroy;
end;

procedure TStopWatch.Start;
begin
  stopWatch_start(FWatch);
end;

procedure TStopWatch.Pause;
begin
  stopWatch_pause(FWatch);
end;

procedure TStopWatch.Resume;
begin
  stopWatch_resume(FWatch);
end;

procedure TStopWatch.Stop;
begin
  stopWatch_stop(FWatch);
end;

procedure TStopWatch.Reset;
begin
  stopWatch_reset(FWatch);
end;

function TStopWatch.GetTick: uint64;
begin
  Result := stopWatch_getTick(FWatch);
end;

function TStopWatch.GetElapsed: elapsed_t;
begin
  Result := stopWatch_getElapsed(FWatch);
end;

function TStopWatch.GetNanoSecondF: double;
begin
  Result := stopWatch_getNanoSecondF(FWatch);
end;

function TStopWatch.GetNanoSecond: uint64;
begin
  Result := stopWatch_getNanoSecond(FWatch);
end;

function TStopWatch.GetMicroSecondF: double;
begin
  Result := stopWatch_getMicroSecondF(FWatch);
end;

function TStopWatch.GetMicroSecond: uint64;
begin
  Result := stopWatch_getMicroSecond(FWatch);
end;

function TStopWatch.GetMillisecondF: double;
begin
  Result := stopWatch_getMillisecondF(FWatch);
end;

function TStopWatch.GetMillisecond: uint64;
begin
  Result := stopWatch_getMillisecond(FWatch);
end;

function TStopWatch.GetSecondF: double;
begin
  Result := stopWatch_getSecondF(FWatch);
end;

function TStopWatch.GetSecond: uint64;
begin
  Result := stopWatch_getSecond(FWatch);
end;

function TStopWatch.GetMinuteF: double;
begin
  Result := stopWatch_getMinuteF(FWatch);
end;

function TStopWatch.GetMinute: uint64;
begin
  Result := stopWatch_getMinute(FWatch);
end;

function TStopWatch.GetHourF: double;
begin
  Result := stopWatch_getHourF(FWatch);
end;

function TStopWatch.GetHour: uint64;
begin
  Result := stopWatch_getHour(FWatch);
end;

function TStopWatch.GetDayF: double;
begin
  Result := stopWatch_getDayF(FWatch);
end;

function TStopWatch.GetDay: uint64;
begin
  Result := stopWatch_getDay(FWatch);
end;

{ THDStopWatch }

function THDStopWatch.CreateWatch(aRunNow: boolean): pstopWatch_t;
begin
  Result := stopWatch_create_hd(aRunNow);
end;

{ TTSCStopWatch }

function TTSCStopWatch.CreateWatch(aRunNow: boolean): pstopWatch_t;
begin
  Result := stopWatch_create_tsc(aRunNow);
end;

procedure TTSCStopWatch.Calibrate;
begin
  stopWatch_tscCalibrate;
end;

initialization

end.
