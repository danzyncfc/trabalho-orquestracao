CREATE TABLE price_snapshot (
    category text PRIMARY KEY,
    avg_price numeric(18, 2) NOT NULL,
    min_price numeric(18, 2) NOT NULL,
    max_price numeric(18, 2) NOT NULL,
    product_count integer NOT NULL CHECK (product_count > 0)
);

CREATE TABLE price_history (
    run_id text NOT NULL,
    logical_date timestamptz NOT NULL,
    category text NOT NULL,
    avg_price numeric(18, 2) NOT NULL,
    min_price numeric(18, 2) NOT NULL,
    max_price numeric(18, 2) NOT NULL,
    product_count integer NOT NULL CHECK (product_count > 0),
    PRIMARY KEY (run_id, category)
);
