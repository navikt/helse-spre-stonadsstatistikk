CREATE TABLE annullering
(
    id            SERIAL PRIMARY KEY,
    fodselsnummer CHAR(11) NOT NULL,
    fagsystem_id  VARCHAR  NOT NULL
);
