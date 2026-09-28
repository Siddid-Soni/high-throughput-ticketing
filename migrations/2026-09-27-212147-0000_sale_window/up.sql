-- When the waiting room starts admitting. Joins before this wait in a lobby
-- and are shuffled into the queue when the sale opens.
ALTER TABLE events ADD COLUMN sale_opens_at TIMESTAMPTZ NOT NULL;
ALTER TABLE events ADD CONSTRAINT events_sale_opens_before_start CHECK (sale_opens_at < start_time);
