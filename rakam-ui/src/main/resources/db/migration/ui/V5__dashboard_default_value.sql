UPDATE "dashboard" SET "shared_everyone" = false WHERE "shared_everyone" IS NULL;

ALTER TABLE "dashboard"
  ALTER COLUMN "shared_everyone" SET DEFAULT false,
  ALTER COLUMN "shared_everyone" SET NOT NULL;