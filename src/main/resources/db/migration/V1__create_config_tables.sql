CREATE TABLE IF NOT EXISTS config_documents (
    id         BIGSERIAL   PRIMARY KEY,
    app        TEXT        NOT NULL,
    env        TEXT        NOT NULL,
    namespace  TEXT        NOT NULL,
    config_key TEXT        NOT NULL,
    content    JSONB       NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by TEXT,
    UNIQUE (app, env, namespace, config_key)
);

CREATE TABLE IF NOT EXISTS config_app_versions (
    app        TEXT   NOT NULL,
    env        TEXT   NOT NULL,
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (app, env)
);
