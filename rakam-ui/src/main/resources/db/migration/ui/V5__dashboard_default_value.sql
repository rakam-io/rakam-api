UPDATE "dashboard"
SET "shared_everyone" = FALSE
WHERE "shared_everyone" IS NULL;

ALTER TABLE "dashboard"
  ALTER COLUMN "shared_everyone" SET DEFAULT FALSE,
  ALTER COLUMN "shared_everyone" SET NOT NULL;