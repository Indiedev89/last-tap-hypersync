-- Create tapped_events table
CREATE TABLE IF NOT EXISTS public.tapped_events (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    tapper_address TEXT NOT NULL,
    round_number TEXT NOT NULL,
    tap_cost_paid TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (transaction_hash, log_index)
);

-- Create round_ended_events table
CREATE TABLE IF NOT EXISTS public.round_ended_events (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    winner_address TEXT NOT NULL,
    prize_amount TEXT NOT NULL,
    round_number TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (transaction_hash, log_index)
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_tapped_round_number
ON public.tapped_events(round_number);

CREATE INDEX IF NOT EXISTS idx_tapped_tapper
ON public.tapped_events(tapper_address);

CREATE INDEX IF NOT EXISTS idx_tapped_timestamp
ON public.tapped_events(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_round_ended_round
ON public.round_ended_events(round_number);

CREATE INDEX IF NOT EXISTS idx_round_ended_winner
ON public.round_ended_events(winner_address);

CREATE INDEX IF NOT EXISTS idx_round_ended_timestamp
ON public.round_ended_events(event_timestamp);