program example;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  Classes,
  SysUtils,
  fafafa.stopWatch { you can add units after this };

const
  COL_NAME_WIDTH = 24;
  COL_TIME_WIDTH = 18;
  COL_MAX_WIDTH = COL_TIME_WIDTH;
  COL_MIN_WIDTH = COL_TIME_WIDTH;
  COL_ITS_WIDTH = COL_TIME_WIDTH;

var
  LWatch: pstopWatch_t;

  procedure ben_for_888;
  var
    i, x: integer;
  begin
    for i := 0 to Pred(888) do
      x := i + 1;
  end;

  procedure ben_for_99999;
  var
    i, x: integer;
  begin
    for i := 0 to Pred(99999) do
      x := i + 1;
  end;


  procedure ben_memset_fillchar;
  var
    LMem: Pointer;
  begin
    LMem := GetMem(512);
    FillChar(LMem^, 512, 0);
    Freemem(LMem);
  end;

  procedure ben_memset_Traverse;
  var
    LMem: Pointer;
    LP: pbyte;
    i: integer;
  begin
    LMem := GetMem(512);
    LP := pbyte(LMem);
    for i := 0 to Pred(512) do
    begin
      LP^ := 0;
      Inc(LP);
    end;
    Freemem(LMem);
  end;

  procedure ben_sleep_50ms;
  begin
    Sleep(50);
  end;

  procedure ben_sleep_20ms;
  begin
    Sleep(20);
  end;

  procedure ben_sleep_220us;
  begin
    USleep(220);
  end;

  procedure ben_sleep_20us;
  begin
    USleep(20);
  end;

  procedure Warmup(aWatch: pstopwatch_t; aBen: TProcedure; aMaxIterations: uint64);
  const
    SIMILARS_ATLEAST = 888;
  var
    LSimilars, LLastTick, LTick: uint64;
    i: integer;
    LDifference: int64;
  begin
    LSimilars := 0;

    stopWatch_reset(aWatch);
    aBen();
    LLastTick := stopWatch_getTick(aWatch);

    for i := 0 to Pred(aMaxIterations) do
    begin
      stopWatch_reset(aWatch);
      aBen();
      LTick := stopWatch_getTick(aWatch);

      if (LTick > 0) then
      begin
        LDifference := int64(LTick) - int64(LLastTick);
        if LDifference < 0 then
          LDifference := +LDifference;

        if LDifference / LTick <= 0.1 then
          Inc(LSimilars);
      end;
      if LSimilars >= SIMILARS_ATLEAST then
        Break;
    end;
  end;

  procedure InternalBen(aWatch: pstopWatch_t; aBen: TProcedure; aUnit: timeUnit_t; out aTime, aMax, aMin: double; out aIterations: uint64);
  const
    SIMILARS_ATLEAST = 888;
    MAX_TimeSec = 8; // 最大测试时间(秒)
    DEFAULT_MAX_ITERATIONS = 9999;
    MAX_ZEROTICKS = 42;
  var
    LSimilars: uint64;
    LMaxIterations: uint64;
    LTick, LLastTick, LAvgTick, LMaxTick, LMin, LRes: uint64;
    LDifference, LZeroTicks: int64;
  begin

    aIterations := 0;
    LSimilars := 0;
    LZeroTicks := 0;

    stopWatch_reset(aWatch);
    aBen();
    LLastTick := stopWatch_getTick(aWatch);

    // 根据首次tick计算最大迭代次数,避免产生固定的过长的测试时间
    LRes := stopWatch_getResolution(aWatch);
    if LLastTick > 0 then
      LMaxIterations := Round(MAX_TimeSec / tickToSecond(LLastTick, LRes))
    else
      LMaxIterations := DEFAULT_MAX_ITERATIONS;

    // 预热 Warmup
    Warmup(aWatch, aBen, LMaxIterations);

    Inc(aIterations);
    LMaxTick := LLastTick;
    LMin := LLastTick;
    LAvgTick := LLastTick;

    while (LSimilars < SIMILARS_ATLEAST) do
    begin
      stopWatch_reset(aWatch);
      aBen();
      LTick := stopWatch_getTick(aWatch);

      if LTick > LMaxTick then
        LMaxTick := LTick;

      if LTick < LMin then
        LMin := LTick;

      LAvgTick := (LLastTick + LTick) div 2;

      // 0 tick 为精度不够,超过指定数量的0tick则跳过测试
      if (LTick > 0) then
      begin
        LDifference := int64(LTick) - int64(LLastTick);
        if LDifference < 0 then
          LDifference := +LDifference;

        if LDifference / LTick <= 0.1 then
          Inc(LSimilars);
      end
      else
      begin
        Inc(LZeroTicks);
        if LZeroTicks >= MAX_ZEROTICKS then
          Break;
      end;

      LLastTick := LTick;
      Inc(aIterations);
      if (aIterations >= LMaxIterations) then
        Break;
    end;

    case aUnit of
      TU_NanoSecond:
      begin
        aTime := tickToNanoSecond(LAvgTick, LRes);
        aMax := tickToNanoSecond(LMaxTick, LRes);
        aMin := tickToNanoSecond(LMin, LRes);
      end;
      TU_MicroSecond:
      begin
        aTime := tickToMicroSecond(LAvgTick, LRes);
        aMax := tickToMicroSecond(LMaxTick, LRes);
        aMin := tickToMicroSecond(LMin, LRes);
      end;
      TU_MilliSecond:
      begin
        aTime := tickToMillisecond(LAvgTick, LRes);
        aMax := tickToMillisecond(LMaxTick, LRes);
        aMin := tickToMillisecond(LMin, LRes);
      end;
      TU_Second:
      begin
        aTime := tickToSecond(LAvgTick, LRes);
        aMax := tickToSecond(LMaxTick, LRes);
        aMin := tickToSecond(LMin, LRes);
      end;
      TU_Minute:
      begin
        aTime := tickToMinute(LAvgTick, LRes);
        aMax := tickToMinute(LMaxTick, LRes);
        aMin := tickToMinute(LMin, LRes);
      end;
      TU_Hour:
      begin
        aTime := tickToHour(LAvgTick, LRes);
        aMax := tickToHour(LMaxTick, LRes);
        aMin := tickToHour(LMin, LRes);
      end;
      TU_Day:
      begin
        aTime := tickToDay(LAvgTick, LRes);
        aMax := tickToDay(LMaxTick, LRes);
        aMin := tickToDay(LMin, LRes);
      end;
    end;
  end;

  procedure InternalEmuBenchmark(aWatch: pstopWatch_t; const aName: string; aProc: TProcedure; aUnit: timeUnit_t);
  var
    LTime, LMax, LMin: double;
    LITerations: uint64;
    LUnitStr: string;
  begin
    LUnitStr := TimeUnitToStr(aUnit);
    Write(Format('%-' + COL_NAME_WIDTH.ToString + 's', [aName]));
    InternalBen(aWatch, aProc, aUnit, LTime, LMax, LMin, LITerations);
    Write(Format('%-' + COL_TIME_WIDTH.ToString + 's', [FormatFloat('0.#####', LTime) + ' ' + LUnitStr]));
    Write(Format('%-' + COL_MAX_WIDTH.ToString + 's', [FormatFloat('0.#####', LMax) + ' ' + LUnitStr]));
    Write(Format('%-' + COL_MIN_WIDTH.ToString + 's', [FormatFloat('0.#####', LMin) + ' ' + LUnitStr]));
    WriteLn(Format('%-' + COL_ITS_WIDTH.ToString + 's', [LITerations.ToString]));
  end;

  procedure emuBenchmark(aWatch: pstopWatch_t; aUnit: timeUnit_t);
  begin
    Write(Format('%-' + COL_NAME_WIDTH.ToString + 's', ['Name']));
    Write(Format('%-' + COL_TIME_WIDTH.ToString + 's', ['Time']));
    Write(Format('%-' + COL_MAX_WIDTH.ToString + 's', ['Max']));
    Write(Format('%-' + COL_MIN_WIDTH.ToString + 's', ['Min']));
    WriteLn(Format('%-' + COL_ITS_WIDTH.ToString + 's', ['Iterations']));

    InternalEmuBenchmark(aWatch, 'ben_for_888', @ben_for_888, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_for_99999', @ben_for_99999, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_memset_fillchar', @ben_memset_fillchar, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_memset_Traverse', @ben_memset_Traverse, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_sleep_50ms', @ben_sleep_50ms, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_sleep_20ms', @ben_sleep_20ms, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_sleep_220us', @ben_sleep_220us, aUnit);
    InternalEmuBenchmark(aWatch, 'ben_sleep_20us', @ben_sleep_20us, aUnit);

    WriteLn('');
  end;

const
  TIME_UNIT = TU_NanoSecond;

begin
  WriteLn('fafafa.stopwatch.example');

  LWatch := stopWatch_create(False);
  WriteLn('Emu Benchmark StopWatch Resolution ',stopWatch_getResolution(LWatch));
  emuBenchmark(LWatch, TIME_UNIT);
  stopWatch_destroy(LWatch);
  WriteLn('');

  LWatch := stopWatch_create_tsc(False);
  WriteLn('Emu Benchmark StopWatchTSC Resolution ', stopWatch_getResolution(LWatch));
  emuBenchmark(LWatch, TIME_UNIT);
  stopWatch_destroy(LWatch);
  WriteLn('');

  LWatch := stopWatch_create_hd(False);
  WriteLn('Emu Benchmark StopWatchHD Resolution ', stopWatch_getResolution(LWatch));
  emuBenchmark(LWatch, TIME_UNIT);
  stopWatch_destroy(LWatch);
  WriteLn('');

end.
