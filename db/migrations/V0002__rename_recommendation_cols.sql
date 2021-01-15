ALTER TABLE recommendation
    RENAME COLUMN article_id TO recommended_article_id,
    DROP COLUMN external_id,
    ADD COLUMN source_article_id INTEGER NOT NULL REFERENCES article (id) ON DELETE CASCADE AFTER model_id,
    ADD UNIQUE (source_article_id, recommended_article_id, model_id),
    ADD INDEX idx_source_article_id_model_id ON recommendation (source_article_id, model_id);
