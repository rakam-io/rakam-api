CREATE TABLE web_user (
  id           SERIAL PRIMARY KEY,
  email        TEXT                        NOT NULL UNIQUE,
  is_activated BOOLEAN DEFAULT FALSE       NOT NULL,
  password     TEXT,
  name         TEXT,
  created_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  gender       CHARACTER VARYING(10),
  user_locale  CHARACTER VARYING(20),
  google_id    CHARACTER VARYING(50),
  read_only    BOOLEAN DEFAULT FALSE,
  stripe_id    CHARACTER VARYING(50),
  CONSTRAINT web_user_email_key UNIQUE (email)
);

CREATE TABLE web_user_project (
  id         SERIAL PRIMARY KEY,
  project    VARCHAR(150)                              NOT NULL,
  api_url    VARCHAR(250),
  user_id    INT REFERENCES web_user (id),
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  timezone   VARCHAR(50),
  CONSTRAINT project_check UNIQUE (project, api_url, user_id)
);

CREATE TABLE web_user_api_key (
  id               SERIAL PRIMARY KEY,
  project_id       INTEGER REFERENCES web_user_project (id),
  scope_expression TEXT,
  user_id          INT REFERENCES web_user (id),
  write_key        TEXT,
  read_key         TEXT                                      NOT NULL,
  master_key       TEXT,
  created_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL
);

CREATE TABLE web_user_api_key_permission (
  api_key_id        INT4                        NOT NULL REFERENCES web_user_api_key (id),
  user_id           INT4                        NOT NULL REFERENCES web_user (id),
  read_permission   BOOLEAN                     NOT NULL,
  write_permission  BOOLEAN                     NOT NULL,
  master_permission BOOLEAN                     NOT NULL,
  scope_expression  TEXT,
  created_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
  PRIMARY KEY (api_key_id, user_id),
  FOREIGN KEY (user_id) REFERENCES web_user (id),
  FOREIGN KEY (api_key_id) REFERENCES web_user_api_key (id)
);

CREATE TABLE reports (
  project_id    INT REFERENCES web_user_project (id) ON UPDATE NO ACTION ON DELETE CASCADE,
  user_id       INT REFERENCES web_user (id),
  slug          VARCHAR(255)                NOT NULL,
  category      VARCHAR(255),
  name          VARCHAR(255)                NOT NULL,
  query         TEXT                        NOT NULL,
  options       TEXT,
  query_options TEXT,
  shared        BOOLEAN                     NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, slug, user_id),
  UNIQUE (project_id, slug),
  CONSTRAINT address UNIQUE (project_id, slug)
);

CREATE TABLE custom_reports (
  report_type VARCHAR(255) NOT NULL,
  user_id     INT REFERENCES web_user (id),
  project_id  INT REFERENCES web_user_project (id) ON UPDATE NO ACTION ON DELETE CASCADE,
  name        VARCHAR(255) NOT NULL,
  data        TEXT         NOT NULL,
  PRIMARY KEY (project_id, report_type, name)
);

CREATE TABLE rakam_cluster (
  user_id  INT REFERENCES web_user (id),
  api_url  VARCHAR(255) NOT NULL,
  lock_key VARCHAR(255),
  PRIMARY KEY (user_id, api_url)
);

CREATE TABLE dashboard (
  id               SERIAL PRIMARY KEY,
  project_id       INT REFERENCES web_user_project (id) ON UPDATE NO ACTION ON DELETE CASCADE,
  user_id          INT REFERENCES web_user (id),
  name             VARCHAR(255) NOT NULL,
  options          TEXT,
  refresh_interval INTEGER,
  shared_everyone  BOOLEAN
);

CREATE TABLE dashboard_items (
  id                  SERIAL PRIMARY KEY,
  dashboard           INT          NOT NULL REFERENCES dashboard (id) ON DELETE CASCADE,
  name                VARCHAR(255) NOT NULL,
  directive           VARCHAR(255) NOT NULL,
  options             TEXT         NOT NULL,
  refresh_interval    INTEGER,
  data                BYTEA,
  last_query_duration INT,
  last_updated        TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE custom_page (
  project_id INT REFERENCES web_user_project (id) ON UPDATE NO ACTION ON DELETE CASCADE,
  name       VARCHAR(255) NOT NULL,
  user_id    INT REFERENCES web_user (id),
  slug       VARCHAR(255) NOT NULL,
  category   VARCHAR(255),
  data       TEXT         NOT NULL,
  PRIMARY KEY (project_id, slug)
);

CREATE TABLE IF NOT EXISTS scheduled_email (
  id               SERIAL PRIMARY KEY,
  project_id       INTEGER REFERENCES web_user_project (id),
  user_id          INTEGER REFERENCES web_user (id),
  name             VARCHAR(255) NOT NULL,
  date_interval    VARCHAR(100) NOT NULL,
  hour_of_day      INTEGER      NOT NULL,
  type             VARCHAR(100),
  type_id          BIGINT,
  created_at       TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
  last_executed_at TIMESTAMP WITHOUT TIME ZONE,
  enabled          BOOL         NOT NULL       DEFAULT TRUE,
  emails           VARCHAR(100) []
);

CREATE TABLE "public"."dashboard_permission" (
  "dashboard" INT4      NOT NULL,
  "user_id"   INT4      NOT NULL,
  "shared_at" TIMESTAMP NULL DEFAULT now(),
  PRIMARY KEY ("dashboard", "user_id")
    NOT DEFERRABLE INITIALLY IMMEDIATE,
  FOREIGN KEY ("dashboard") REFERENCES "public"."dashboard" ("id") ON DELETE CASCADE,
  FOREIGN KEY ("user_id") REFERENCES "public"."web_user" ("id") ON DELETE CASCADE
);

CREATE TABLE ui_user_defaults (
  user_id    INT REFERENCES web_user (id) ON DELETE CASCADE,
  project_id INTEGER REFERENCES web_user_project (id) ON DELETE CASCADE,
  name       VARCHAR(100),
  value      TEXT,
  created_at TIMESTAMP DEFAULT now() NOT NULL,
  PRIMARY KEY (user_id, project_id, name)
)