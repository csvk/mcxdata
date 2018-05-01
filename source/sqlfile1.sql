-- Sample SQL Queries

SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate
                   FROM tblDump 
                  WHERE InstrumentName = "FUTCOM"
                    AND Symbol = "COTTON"


CREATE TABLE "tblDump2" ( 
  `Date` TEXT, `InstrumentName` TEXT, 
  `Symbol` TEXT, `ExpiryDate` TEXT, 
  `OptionType` TEXT, 
  `StrikePrice` INTEGER, 
  `Open` REAL, 
  `High` REAL, 
  `Low` REAL, 
  `Close` REAL, 
  `PreviousClose` REAL, 
  `VolumeLots` INTEGER, 
  `VolumeThousands` TEXT, 
  `Value` REAL, 
  `OpenInterestLots` INTEGER )

  CREATE TABLE "tblExpiries" ( `Symbol` TEXT, `ExpiryDate` TEXT )

  CREATE TABLE "tblFutures" ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Open` REAL, 
    `High` REAL, 
    `Low` REAL, 
    `Close` REAL, 
    `VolumeLots` INTEGER, 
    `OpenInterestLots` INTEGER, 
    `ExpiryDate` TEXT)

CREATE INDEX `idxFutures` ON `tblFutures` ( `Symbol` ASC, `Date` ASC, `ExpiryDate` ASC)
    
CREATE INDEX `idxDump` ON `tblDump` ( `Symbol` ASC, `Date` ASC, `ExpiryDate` ASC, `InstrumentName` ASC )

select symbol, count(*) from tblFutures group by symbol

select symbol, count(*) from (select distinct symbol, date from tblDump) group by symbol
    