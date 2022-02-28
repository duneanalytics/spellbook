CREATE SCHEMA IF NOT EXISTS chainlink;

CREATE TABLE IF NOT EXISTS chainlink.view_price_feeds(
    hour timestamptz NOT NULL,
    feed_name text NOT NULL,
    price numeric NOT NULL,
    underlying_token_address bytea,
        UNIQUE (hour,feed_name,underlying_token_address)
);


CREATE INDEX IF NOT EXISTS chainlink_view_price_feeds_underlying_feed_hour_idx ON chainlink.view_price_feeds (underlying_token_address, hour, feed_name);
CREATE INDEX IF NOT EXISTS chainlink_view_price_feeds_underlying_hour_idx ON chainlink.view_price_feeds (underlying_token_address, hour);
CREATE INDEX IF NOT EXISTS chainlink_view_price_feeds_feed_hour_idx ON chainlink.view_price_feeds (hour, feed_name);
CREATE INDEX IF NOT EXISTS chainlink_view_price_feeds_hour_idx ON chainlink.view_price_feeds USING BRIN (hour);
