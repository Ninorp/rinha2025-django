-- Initialize only the schema required by the Django payments.Payment model
-- This script is intended to run during the first initialization of the Postgres container

CREATE TABLE IF NOT EXISTS payments_payment (
    "correlationId" varchar(255) PRIMARY KEY,
    "amount" numeric(10,2) NOT NULL,
    "status" varchar(20) NOT NULL DEFAULT 'pending',
    "gatewayIdentifier" varchar(255) NULL,
    "created_at" timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS payment_index ON payments_payment ("created_at", "status");
