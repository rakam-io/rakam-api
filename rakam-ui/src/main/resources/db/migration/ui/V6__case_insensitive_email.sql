CREATE UNIQUE INDEX unique_user_emails
  ON web_user (lower(email))