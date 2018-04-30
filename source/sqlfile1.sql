-- Sample SQL Queries

SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate
                   FROM tblDump 
                  WHERE InstrumentName = "FUTCOM"
                    AND Symbol = "COTTON"