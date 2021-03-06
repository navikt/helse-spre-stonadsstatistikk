CREATE TABLE hendelse
(
    dokument_id UUID,
    hendelse_id UUID,
    type        VARCHAR,
    PRIMARY KEY (dokument_id, hendelse_id, type)
);

CREATE INDEX hendelse_hendelse_id_idx ON hendelse (hendelse_id);

CREATE TABLE vedtak
(
    id                    SERIAL PRIMARY KEY,
    fodselsnummer         CHAR(11)  NOT NULL,
    organisasjonsnummer   CHAR(9)   NOT NULL,
    utbetalingstidspunkt  TIMESTAMP NOT NULL,
    fom                   DATE      NOT NULL,
    tom                   DATE      NOT NULL,
    forbrukte_sykedager   INTEGER   NOT NULL,
    gjenstaende_sykedager INTEGER   NOT NULL,
    maksdato              DATE,
    sykmelding_id         UUID      NOT NULL,
    soknad_id             UUID      NOT NULL,
    inntektsmelding_id    UUID,
    hendelse_id           UUID      NOT NULL
);

CREATE TABLE oppdrag
(
    id           SERIAL PRIMARY KEY,
    vedtak_id    INTEGER NOT NULL REFERENCES vedtak (id),
    mottaker     VARCHAR,
    fagomrade    VARCHAR,
    fagsystem_id VARCHAR,
    totalbelop   INTEGER
);

CREATE TABLE utbetaling
(
    id         SERIAL PRIMARY KEY,
    oppdrag_id INTEGER NOT NULL REFERENCES oppdrag (id),
    fom        DATE    NOT NULL,
    tom        DATE    NOT NULL,
    dagsats    INTEGER NOT NULL,
    grad       DECIMAL NOT NULL,
    belop      INTEGER NOT NULL,
    sykedager  INTEGER NOT NULL
);

CREATE TABLE vedtak_utbetalingsref
(
    vedtaksperiode_id UUID     NOT NULL,
    utbetalingsref    CHAR(26) NOT NULL,
    maksdato          DATE     NOT NULL,
    PRIMARY KEY (vedtaksperiode_id)
);

CREATE INDEX utbetalingsref_idx ON vedtak_utbetalingsref (utbetalingsref);

CREATE TABLE annullering
(
    id                    SERIAL PRIMARY KEY,
    fodselsnummer         CHAR(11)  NOT NULL,
    fagsystem_id          VARCHAR   NOT NULL,
    annulleringstidspunkt TIMESTAMP NOT NULL
);
