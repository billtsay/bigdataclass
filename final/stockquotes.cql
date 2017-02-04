/*
Schema for storing average stock price data.
*/

DROP KEYSPACE IF EXISTS cs9223;
CREATE KEYSPACE cs9223 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use cs9223;

CREATE TABLE stockquotes (
    symbol text,            // Stock symbol
    price double,    		// average price 
    eventtime timestamp,   // event time
	PRIMARY KEY (symbol,eventtime)
);
