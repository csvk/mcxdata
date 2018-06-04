-- Sample SQL Queries

SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate
                   FROM tblDump 
                  WHERE InstrumentName = "FUTCOM"
                    AND Symbol = "COTTON"


CREATE TABLE "tblDump" ( `Date` TEXT, `InstrumentName` TEXT, `Symbol` TEXT, `ExpiryDate` TEXT, `OptionType` TEXT,
`StrikePrice` INTEGER, `Open` REAL, `High` REAL, `Low` REAL, `Close` REAL, `PreviousClose` REAL, `VolumeLots` INTEGER,
`VolumeThousands` TEXT, `Value` REAL, `OpenInterestLots` INTEGER )

CREATE TABLE "tblDumpStaging" ( `Date` TEXT, `InstrumentName` TEXT, `Symbol` TEXT, `ExpiryDate` TEXT, `OptionType` TEXT,
 `StrikePrice` INTEGER, `Open` REAL, `High` REAL, `Low` REAL, `Close` REAL, `PreviousClose` REAL, `VolumeLots` INTEGER,
 `VolumeThousands` TEXT, `Value` REAL, `OpenInterestLots` INTEGER )

CREATE TABLE "tblExpiries" ( `Symbol` TEXT, `ExpiryDate` TEXT )

CREATE TABLE "tblFutures" ( `Symbol` TEXT, `Date` TEXT, `Open` REAL, `High` REAL, `Low` REAL, `Close` REAL,
`VolumeLots` INTEGER, `OpenInterestLots` INTEGER, `ExpiryDate` TEXT, PRIMARY KEY(`Symbol`,`Date`) )

CREATE TABLE "tblMultipliers" ( `Symbol` TEXT, `RolloverDate` TEXT, `PreviousExpiry` TEXT, `NextExpiry` TEXT,
`DumpClose` REAL, `FuturesClose` REAL, `MultiplierCalcType` TEXT, `MultiplierCalcDate` TEXT,
`DaysBetweenCalcRollover` INTEGER, `Multiplier` REAL, `ResultantMultiplier` REAL, PRIMARY KEY(`Symbol`,`RolloverDate`) )

CREATE TABLE "tblContract" ( `Symbol` TEXT, `Date` TEXT, `Open` REAL, `High` REAL, `Low` REAL, `Close` REAL,
`VolumeLots` INTEGER, `OpenInterestLots` INTEGER, `ExpiryDate` TEXT, `AdjustedOpen` REAL, `AdjustedHigh` REAL,
`AdjustedLow` REAL, `AdjustedClose` REAL, PRIMARY KEY(`Symbol`,`Date`) )

CREATE INDEX `idxDump` ON `tblDump` ( `Symbol` ASC, `Date` ASC, `ExpiryDate` ASC, `InstrumentName` ASC )

CREATE INDEX `idxDumpStaging` ON `tblDumpStaging` ( `Symbol` ASC, `Date` ASC, `ExpiryDate` ASC, `InstrumentName` ASC )

CREATE INDEX `idxFutures` ON `tblFutures` ( `Symbol` ASC, `Date` ASC, `ExpiryDate` ASC )

CREATE UNIQUE INDEX `idxMultipliers` ON `tblContract` ( `Symbol` ASC, `Date` ASC )

CREATE UNIQUE INDEX `idxContract` ON `tblContract` ( `Symbol` ASC, `Date` ASC )

select symbol, count(*) from tblFutures group by symbol

select symbol, count(*) from (select distinct symbol, date from tblDump) group by symbol

-- Find rows missing in tblFutures
SELECT tblDump.Symbol, tblDump.Date, tblDump.ExpiryDate, tblDump.VolumeLots
                   FROM tblDump LEFT OUTER JOIN tblFutures
                     ON tblDump.Symbol = tblFutures.Symbol
                    AND tblDump.Date = tblFutures.Date
                  WHERE tblFutures.date is NULL
                  ORDER BY tblDump.Symbol ASC, tblDump.ExpiryDate ASC, tblDump.Date ASC


-- date duplicate sanity check
SELECT *
  FROM (SELECT Symbol, Date, Count(*) Date_Count
          FROM tblFutures
         GROUP BY Symbol, Date) tblData
 WHERE Date_Count = 1

 select symbol, date, count(*) days from tblFutures group by symbol, date order by days DESC
    