ALTER TABLE "public".dashboard add column shared_everyone bool;
CREATE TABLE "public"."dashboard_permission" (
	"dashboard" int4 NOT NULL,
	"user_id" int4 NOT NULL,
	"shared_at" timestamp NULL DEFAULT now(),
	PRIMARY KEY ("dashboard", "user_id") NOT DEFERRABLE INITIALLY IMMEDIATE,
	FOREIGN KEY ("dashboard") REFERENCES "public"."dashboard" ("id") ON DELETE CASCADE,
	FOREIGN KEY ("user_id") REFERENCES "public"."web_user" ("id") ON DELETE CASCADE
);