CREATE TABLE article(
   id SERIAL PRIMARY KEY,
   -- reference id for article in external newsroom system
   external_id INTEGER NOT NULL,
   title TEXT NOT NULL DEFAULT '',
   published_at TIMESTAMPTZ DEFAULT NULL,
   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
   updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_external_id ON article (external_id);

CREATE TABLE model (
  id SERIAL PRIMARY KEY,
  -- type of entity a model is recommending articles for - "article", "user," etc
  type TEXT NOT NULL,
  -- status of the model - "pending," "current," etc
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE recommendation (
  id SERIAL PRIMARY KEY,
  -- represents the entity being recommended for. could be an article, user, etc
  external_id TEXT NOT NULL,
  model_id INTEGER NOT NULL REFERENCES model (id) ON DELETE CASCADE,
  -- represents the article recommended for a given external_id
  article_id INTEGER NOT NULL REFERENCES article (id) ON DELETE CASCADE,
  -- how relevant the article is for the entity
  score DECIMAL (7, 6) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (external_id, model_id, article_id)
);

CREATE INDEX idx_external_id_model_id ON recommendation (external_id, model_id);
