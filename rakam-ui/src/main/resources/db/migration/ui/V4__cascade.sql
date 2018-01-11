ALTER TABLE "scheduled_email"
  DROP CONSTRAINT IF EXISTS "scheduled_email_project_id_fkey";
ALTER TABLE "scheduled_email"
  ADD CONSTRAINT "scheduled_email_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."web_user_project" ("id") ON UPDATE NO ACTION ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "web_user_api_key"
  DROP CONSTRAINT IF EXISTS "web_user_api_key_project_id_fkey";
ALTER TABLE "web_user_api_key"
  ADD CONSTRAINT "web_user_api_key_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "public"."web_user_project" ("id") ON UPDATE NO ACTION ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE;