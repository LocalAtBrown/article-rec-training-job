DROP TABLE recommendation;

CREATE TABLE recommendation (
  id SERIAL PRIMARY KEY,
  model_id INTEGER NOT NULL REFERENCES model (id) ON DELETE CASCADE,
  -- represents the entity being recommended for. could be an article, user, etc
  source_entity_id TEXT NOT NULL,
  -- represents the article recommended for the source entity
  recommended_article_id INTEGER NOT NULL REFERENCES article (id) ON DELETE CASCADE,
  -- how relevant the article is for the entity
  score DECIMAL (7, 6) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (model_id, source_entity_id, recommended_article_id)
);
