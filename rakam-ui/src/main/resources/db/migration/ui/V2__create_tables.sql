CREATE TABLE web_user (
  id SERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  is_activated BOOLEAN DEFAULT false NOT NULL,
  password TEXT,
  name TEXT,
  created_at timestamp without time zone NOT NULL,
  gender character varying(10),
  user_locale character varying(20),
  google_id character varying(50),
  read_only boolean DEFAULT false,
  stripe_id character varying(50),
  CONSTRAINT web_user_email_key UNIQUE (email)
);

CREATE TABLE web_user_project (
  id serial PRIMARY KEY,
  project varchar(150) NOT NULL,
  api_url varchar(250),
  user_id INT REFERENCES web_user(id),
  created_at timestamp without time zone DEFAULT now() NOT NULL,
  timezone varchar(50),
  CONSTRAINT project_check UNIQUE(project, api_url, user_id)
);

CREATE TABLE web_user_api_key (
  id SERIAL PRIMARY KEY,
  project_id INTEGER REFERENCES web_user_project(id),
  scope_expression TEXT,
  user_id INT REFERENCES web_user(id),
  write_key TEXT,
  read_key TEXT NOT NULL,
  master_key TEXT,
  created_at timestamp without time zone DEFAULT now() NOT NULL
);

CREATE TABLE web_user_api_key_permission (
  api_key_id int4 NOT NULL REFERENCES web_user_api_key(id),
  user_id int4 NOT NULL REFERENCES web_user(id),
  read_permission boolean not null,
  write_permission boolean not null,
  master_permission boolean not null,
  scope_expression text,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  PRIMARY KEY (api_key_id, user_id),
  FOREIGN KEY (user_id) REFERENCES web_user (id),
  FOREIGN KEY (api_key_id) REFERENCES web_user_api_key (id)
);

CREATE TABLE reports (
  project_id INT REFERENCES web_user_project(id) ON UPDATE NO ACTION ON DELETE CASCADE,
  user_id INT REFERENCES web_user(id),
  slug VARCHAR(255) NOT NULL,
  category VARCHAR(255),
  name VARCHAR(255) NOT NULL,
  query TEXT NOT NULL,
  options TEXT,
  query_options TEXT,
  shared BOOLEAN NOT NULL DEFAULT false,
  created_at timestamp without time zone NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, slug, user_id),
  UNIQUE (project_id, slug),
  CONSTRAINT address UNIQUE(project_id, slug)
);

CREATE TABLE custom_reports (
  report_type VARCHAR(255) NOT NULL,
  user_id INT REFERENCES web_user(id),
  project_id INT REFERENCES web_user_project(id) ON UPDATE NO ACTION ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  data TEXT NOT NULL,
  PRIMARY KEY (project_id, report_type, name)
);

CREATE TABLE rakam_cluster (
  user_id INT REFERENCES web_user(id),
  api_url VARCHAR(255) NOT NULL,
  lock_key VARCHAR(255),
  PRIMARY KEY (user_id, api_url)
);

CREATE TABLE dashboard (
  id SERIAL PRIMARY KEY,
  project_id INT REFERENCES web_user_project(id) ON UPDATE NO ACTION ON DELETE CASCADE,
  user_id INT REFERENCES web_user(id),
  name VARCHAR(255) NOT NULL,
  options TEXT,
  refresh_interval integer,
  shared_everyone boolean
);

CREATE TABLE dashboard_items (
  id SERIAL PRIMARY KEY,
  dashboard int NOT NULL REFERENCES dashboard(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  directive VARCHAR(255) NOT NULL,
  options text NOT NULL,
  refresh_interval integer,
  data bytea,
  last_query_duration int,
  last_updated timestamp without time zone
);

CREATE TABLE custom_page (
  project_id INT REFERENCES web_user_project(id) ON UPDATE NO ACTION ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  user_id INT REFERENCES web_user(id),
  slug VARCHAR(255) NOT NULL,
  category VARCHAR(255),
  data TEXT NOT NULL,
  PRIMARY KEY (project_id, slug)
);

CREATE TABLE IF NOT EXISTS scheduled_email (
  id SERIAL PRIMARY KEY,
  project_id INTEGER REFERENCES web_user_project(id),
  user_id INTEGER REFERENCES web_user(id),
  name VARCHAR(255) NOT NULL,
  date_interval VARCHAR(100) NOT NULL,
  hour_of_day INTEGER NOT NULL,
  type VARCHAR(100),
  type_id BIGINT,
  created_at timestamp without time zone default (now() at time zone 'utc'),
  last_executed_at timestamp without time zone,
  enabled BOOL NOT NULL DEFAULT true,
  emails VARCHAR(100)[]  );

CREATE TABLE "public"."dashboard_permission" (
  "dashboard" int4 NOT NULL,
  "user_id" int4 NOT NULL,
  "shared_at" timestamp NULL DEFAULT now(),
  PRIMARY KEY ("dashboard", "user_id") NOT DEFERRABLE INITIALLY IMMEDIATE,
  FOREIGN KEY ("dashboard") REFERENCES "public"."dashboard" ("id") ON DELETE CASCADE,
  FOREIGN KEY ("user_id") REFERENCES "public"."web_user" ("id") ON DELETE CASCADE
);

CREATE TABLE ui_user_defaults (
  user_id INT REFERENCES web_user(id) ON DELETE CASCADE,
  project_id INTEGER REFERENCES web_user_project(id) ON DELETE CASCADE,
  name varchar(100),
  value TEXT,
  created_at timestamp DEFAULT now() NOT NULL,
  PRIMARY KEY (user_id, project_id, name)
)