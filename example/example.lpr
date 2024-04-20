program example;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  Classes,
  SysUtils,
  DateUtils,
  Math,
  fafafa.snowFlake;

const
  STR_SNOWFLAKEID = 'SnowFlakeID';
  STR_SEQUENCE = 'Sequence';
  STR_WORKERID = 'WorkerID';
  STR_DATACENTERID = 'DataCenterID';
  STR_TIMESTAMP = 'TimeStamp';


  function formatDateTime(aDataTime: TDateTime): string;
  var
    LYear, LMonth, LDay, LHour, LMinute, LSecond, LMS: word;
  begin
    DecodeDateTime(aDataTime, LYear, LMonth, LDay, LHour, LMinute, LSecond, LMS);
    Result := Concat(LYear.ToString, '/', LMonth.ToString, '/', LDay.ToString, ' ', LHour.ToString, ':', LMinute.ToString, ':', LSecond.ToString, ':', LMS.ToString);
  end;

  function CountDigits(const Value: int64): integer;
  begin
    if Value = 0 then
      Result := 1
    else
      Result := Floor(Log10(Abs(Value))) + 1;
  end;

  function Max(a, b: uint8): uint8;
  begin
    if a > b then
      Result := a
    else
      Result := b;
  end;

  procedure snowFlakeGen_exampe(aCount: uint16; aWorkerID: workerID_t; aDataCenterID: dataCenterID_t; aEpoch: TDateTime);
  var
    HEADER_WIDTH_ID, HEADER_WIDTH_SEQ, HEADER_WIDTH_WORKERID, HEADER_WIDTH_DATACENTERID, HEADER_WIDTH_TIMESTAMP: uint8;

    LGen: ISnowFlakeGen;
    i: integer;
    LID: int64;
    LSnowFlake: snowFlake_t;
  begin
    LGen := makeSnowFlakeGen(aWorkerID, aDataCenterID, aEpoch);
    WriteLn('Generate SnowFlakeID WorkerID:', LGen.WorkerID, ' DataCenterID:', LGen.DatacenterId, ' Epoch:', formatDateTime(LGen.Epoch), ' Count:', aCount);
    WriteLn('');

    { 表头 }
    HEADER_WIDTH_ID := Max(CountDigits(High(int64)), Length(STR_SNOWFLAKEID));
    HEADER_WIDTH_SEQ := Max(Length(STR_SEQUENCE), 4);
    HEADER_WIDTH_WORKERID := Max(Length(STR_WORKERID), 2);
    HEADER_WIDTH_DATACENTERID := Max(Length(STR_DATACENTERID), 2);
    HEADER_WIDTH_TIMESTAMP := HEADER_WIDTH_ID + Length('####/##/## ##:##:##:##');
    WriteLn(Format('%-' + HEADER_WIDTH_ID.ToString + 's %-' + HEADER_WIDTH_SEQ.ToString + 's %-' + HEADER_WIDTH_WORKERID.ToString + 's %-' + HEADER_WIDTH_DATACENTERID.ToString + 's %-' + HEADER_WIDTH_TIMESTAMP.ToString + 's', [STR_SNOWFLAKEID, STR_SEQUENCE, STR_WORKERID, STR_DATACENTERID, STR_TIMESTAMP]));

    for i := 0 to Pred(aCount) do
    begin
      LID := LGen.NextId;
      LSnowFlake := snowFlake(LID);
      WriteLn(Format('%-' + HEADER_WIDTH_ID.ToString + 's %-' + HEADER_WIDTH_SEQ.ToString + 's %-' + HEADER_WIDTH_WORKERID.ToString + 's %-' + HEADER_WIDTH_DATACENTERID.ToString + 's %-' + HEADER_WIDTH_TIMESTAMP.ToString + 's', [LID.ToString, LSnowFlake.sequence.ToString, IntToStr(LSnowFlake.workerID), IntToStr(LSnowFlake.dataCenterID), LSnowFlake.timestamp.ToString + '(' + formatDateTime(LGen.GetTime(LID)) + ')']));

    end;

    WriteLn('');
  end;

  //procedure simpleExample;
  //var
  //  LGen: ISnowFlakeGen;
  //  i: integer;
  //  LID: int64;
  //begin
  //  LGen := makeSnowFlakeGen(2, 2, Now);
  //  for i := 0 to Pred(9) do
  //  begin
  //    LGen.Lock;
  //    LID := LGen.GetNextId;
  //    LGen.UnLock;
  //  end;
  //end;

begin
  WriteLn('fafafa.snowFlake.example');
  WriteLn('');
  snowFlakeGen_exampe(8, 1, 1, Now);
  snowFlakeGen_exampe(8, 2, 2, EncodeDate(2024, 4, 20));
  snowFlakeGen_exampe(8, 3, 3, EncodeDate(2008, 4, 18));
  snowFlakeGen_exampe(8, 4, 1, EncodeDate(2024, 01, 01));
  snowFlakeGen_exampe(888, 30, 30, EncodeDate(1999, 12, 01));

  WriteLn('bye');

end.
